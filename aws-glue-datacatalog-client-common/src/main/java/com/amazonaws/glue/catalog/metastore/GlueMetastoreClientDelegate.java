package com.amazonaws.glue.catalog.metastore;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.glue.catalog.converters.CatalogToHiveConverter;
import com.amazonaws.glue.catalog.converters.GlueInputConverter;
import com.amazonaws.glue.catalog.converters.HiveToCatalogConverter;
import com.amazonaws.glue.catalog.util.BatchCreatePartitionsHelper;
import com.amazonaws.glue.catalog.util.ExpressionHelper;
import com.amazonaws.glue.catalog.util.MetastoreClientUtils;
import com.amazonaws.glue.catalog.util.PartitionKey;
import com.amazonaws.glue.shims.AwsGlueHiveShims;
import com.amazonaws.glue.shims.ShimsLoader;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.BatchGetPartitionRequest;
import com.amazonaws.services.glue.model.BatchGetPartitionResult;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.CreateUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.DeletePartitionRequest;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.DeleteUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.amazonaws.services.glue.model.GetDatabaseResult;
import com.amazonaws.services.glue.model.GetDatabasesRequest;
import com.amazonaws.services.glue.model.GetDatabasesResult;
import com.amazonaws.services.glue.model.GetPartitionRequest;
import com.amazonaws.services.glue.model.GetPartitionResult;
import com.amazonaws.services.glue.model.GetPartitionsRequest;
import com.amazonaws.services.glue.model.GetPartitionsResult;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.GetTableResult;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionResult;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionsRequest;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionsResult;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.PartitionValueList;
import com.amazonaws.services.glue.model.Segment;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UpdateDatabaseRequest;
import com.amazonaws.services.glue.model.UpdatePartitionRequest;
import com.amazonaws.services.glue.model.UpdateTableRequest;
import com.amazonaws.services.glue.model.UpdateUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.UserDefinedFunction;
import com.amazonaws.services.glue.model.UserDefinedFunctionInput;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.MetadataPpdResult;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import static com.amazonaws.glue.catalog.converters.ConverterUtils.stringToCatalogTable;
import static com.amazonaws.glue.catalog.util.MetastoreClientUtils.deepCopyMap;
import static com.amazonaws.glue.catalog.util.MetastoreClientUtils.isExternalTable;
import static com.amazonaws.glue.catalog.util.MetastoreClientUtils.makeDirs;
import static com.amazonaws.glue.catalog.util.MetastoreClientUtils.validateGlueTable;
import static com.amazonaws.glue.catalog.util.MetastoreClientUtils.validateTableObject;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.hive.metastore.HiveMetaStore.PUBLIC;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;
import static org.apache.hadoop.hive.metastore.TableType.MANAGED_TABLE;

/***
 * Delegate Class to provide all common functionality
 * between Spark-hive version, Hive and Presto clients
 *
 */
public class GlueMetastoreClientDelegate {

  private static final Logger logger = Logger.getLogger(GlueMetastoreClientDelegate.class);

  private static final List<Role> implicitRoles = Lists.newArrayList(new Role(PUBLIC, 0, PUBLIC));
  public static final int MILLISECOND_TO_SECOND_FACTOR = 1000;
  public static final Long NO_MAX = -1L;
  public static final String MATCH_ALL = ".*";

  public static final String INDEX_PREFIX = "index_prefix";

  private static final int BATCH_CREATE_PARTITIONS_MAX_REQUEST_SIZE = 100;
  private static final int BATCH_GET_PARTITIONS_MAX_REQUEST_SIZE = 1000;
  /**
   * Based on the maxResults parameter at https://docs.aws.amazon.com/glue/latest/webapi/API_GetPartitions.html
   */
  public static final int GET_PARTITIONS_MAX_SIZE = 1000;

  public static final String CUSTOM_EXECUTOR_FACTORY_CONF = "hive.metastore.executorservice.factory.class";

  static final String GLUE_METASTORE_DELEGATE_THREADPOOL_NAME_FORMAT = "glue-metastore-delegate-%d";

  /**
   * Maximum number of Glue Segments. A segment defines a non-overlapping region of a table's partitions,
   * allowing multiple requests to be executed in parallel.
   */
  public static final int DEFAULT_NUM_PARTITION_SEGMENTS = 5;

  /**
   * Currently the upper limit allowed by Glue is 10.
   *
   * @see https://docs.aws.amazon.com/glue/latest/webapi/API_Segment.html
   */
  public static final int MAX_NUM_PARTITION_SEGMENTS = 10;

  private final ExecutorService executorService;
  private final AWSGlue glueClient;
  private final HiveConf conf;
  private final Warehouse wh;
  private final AwsGlueHiveShims hiveShims = ShimsLoader.getHiveShims();
  private final String catalogId;
  private final int numPartitionSegments;
  
  public static final String CATALOG_ID_CONF = "hive.metastore.glue.catalogid";
  public static final String NUM_PARTITION_SEGMENTS_CONF = "aws.glue.partition.num.segments";

  protected ExecutorService getExecutorService() {
    Class<? extends ExecutorServiceFactory> executorFactoryClass = this.conf
            .getClass(CUSTOM_EXECUTOR_FACTORY_CONF,
                    DefaultExecutorServiceFactory.class).asSubclass(
                    ExecutorServiceFactory.class);
    ExecutorServiceFactory factory = ReflectionUtils.newInstance(
            executorFactoryClass, conf);
    return factory.getExecutorService(conf);
  }

  public GlueMetastoreClientDelegate(HiveConf conf, AWSGlue glueClient, Warehouse wh) throws MetaException {
    checkNotNull(conf, "Hive Config cannot be null");
    checkNotNull(glueClient, "glueClient cannot be null");
    checkNotNull(wh, "Warehouse cannot be null");
    numPartitionSegments = conf.getInt(NUM_PARTITION_SEGMENTS_CONF, DEFAULT_NUM_PARTITION_SEGMENTS);
    checkArgument(numPartitionSegments <= MAX_NUM_PARTITION_SEGMENTS,
            String.format("Hive Config [%s] can't exceed %d", NUM_PARTITION_SEGMENTS_CONF, MAX_NUM_PARTITION_SEGMENTS));

    this.conf = conf;
    this.glueClient = glueClient;
    this.wh = wh;
    this.executorService = getExecutorService();

    // TODO - May be validate catalogId confirms to AWS AccountId too.
    catalogId = MetastoreClientUtils.getCatalogId(conf);
  }

  // ======================= Database =======================

  public void createDatabase(org.apache.hadoop.hive.metastore.api.Database database) throws TException {
    checkNotNull(database, "database cannot be null");

    if (StringUtils.isEmpty(database.getLocationUri())) {
      database.setLocationUri(wh.getDefaultDatabasePath(database.getName()).toString());
    } else {
      database.setLocationUri(wh.getDnsPath(new Path(database.getLocationUri())).toString());
    }
    Path dbPath = new Path(database.getLocationUri());
    boolean madeDir = makeDirs(wh, dbPath);

    try {
      DatabaseInput catalogDatabase = GlueInputConverter.convertToDatabaseInput(database);
      CreateDatabaseRequest createDatabaseRequest = new CreateDatabaseRequest().withDatabaseInput(catalogDatabase)
          .withCatalogId(catalogId);
      glueClient.createDatabase(createDatabaseRequest);
    } catch (AmazonServiceException e) {
      if (madeDir) {
        wh.deleteDir(dbPath, true);
      }
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to create database: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  public org.apache.hadoop.hive.metastore.api.Database getDatabase(String name) throws TException {
    checkArgument(StringUtils.isNotEmpty(name), "name cannot be null or empty");

    try {
      GetDatabaseRequest getDatabaseRequest = new GetDatabaseRequest().withName(name).withCatalogId(catalogId);
      GetDatabaseResult result = glueClient.getDatabase(getDatabaseRequest);
      Database catalogDatabase = result.getDatabase();
      return CatalogToHiveConverter.convertDatabase(catalogDatabase);
    } catch (AmazonServiceException e) {
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e){
      String msg = "Unable to get database object: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  public List<String> getDatabases(String pattern) throws TException {
    // Special handling for compatibility with Hue that passes "*" instead of ".*"
    if (pattern == null || pattern.equals("*")) {
      pattern = MATCH_ALL;
    }

    try {
      List<String> ret = Lists.newArrayList();
      String nextToken = null;

      do {
        GetDatabasesRequest getDatabasesRequest = new GetDatabasesRequest().withNextToken(nextToken).withCatalogId(
            catalogId);
        GetDatabasesResult result = glueClient.getDatabases(getDatabasesRequest);
        nextToken = result.getNextToken();

        for (Database db : result.getDatabaseList()) {
          String name = db.getName();
          if (Pattern.matches(pattern, name)) {
            ret.add(name);
          }
        }
      } while (nextToken != null);
      return ret;
    } catch (AmazonServiceException e) {
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e){
      String msg = "Unable to get databases: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  public void alterDatabase(String databaseName, org.apache.hadoop.hive.metastore.api.Database database) throws TException {
    checkArgument(StringUtils.isNotEmpty(databaseName), "databaseName cannot be null or empty");
    checkNotNull(database, "database cannot be null");

    try {
      DatabaseInput catalogDatabase = GlueInputConverter.convertToDatabaseInput(database);
      UpdateDatabaseRequest updateDatabaseRequest = new UpdateDatabaseRequest().withName(databaseName)
          .withDatabaseInput(catalogDatabase).withCatalogId(catalogId);
      glueClient.updateDatabase(updateDatabaseRequest);
    } catch (AmazonServiceException e) {
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e){
      String msg = "Unable to alter database: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade) throws TException {
    checkArgument(StringUtils.isNotEmpty(name), "name cannot be null or empty");

    String dbLocation;
    try {
      List<String> tables = getTables(name, MATCH_ALL);
      boolean isEmptyDatabase = tables.isEmpty();

      org.apache.hadoop.hive.metastore.api.Database db = getDatabase(name);
      dbLocation = db.getLocationUri();

      // TODO: handle cascade
      if (isEmptyDatabase || cascade) {
        DeleteDatabaseRequest deleteDatabaseRequest = new DeleteDatabaseRequest().withName(name).withCatalogId(
            catalogId);
        glueClient.deleteDatabase(deleteDatabaseRequest);
      } else {
        throw new InvalidOperationException("Database " + name + " is not empty.");
      }
    } catch (NoSuchObjectException e) {
      if (ignoreUnknownDb) {
        return;
      } else {
        throw e;
      }
    } catch (AmazonServiceException e) {
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e){
      String msg = "Unable to drop database: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }

    if (deleteData) {
      try {
        wh.deleteDir(new Path(dbLocation), true);
      } catch (Exception e) {
        logger.error("Unable to remove database directory " + dbLocation, e);
      }
    }
  }

  public boolean databaseExists(String dbName) throws TException {
    checkArgument(StringUtils.isNotEmpty(dbName), "dbName cannot be null or empty");

    try {
      getDatabase(dbName);
    } catch (NoSuchObjectException e) {
      return false;
    } catch (AmazonServiceException e) {
      throw new TException(e);
    } catch (Exception e) {
      throw new MetaException(e.getMessage());
    }
    return true;
  }

  // ======================== Table ========================

  public void createTable(org.apache.hadoop.hive.metastore.api.Table tbl) throws TException {
    checkNotNull(tbl, "tbl cannot be null");
    boolean dirCreated = validateNewTableAndCreateDirectory(tbl);
    try {
      // Glue Server side does not set DDL_TIME. Set it here for the time being.
      // TODO: Set DDL_TIME parameter in Glue service
      tbl.setParameters(deepCopyMap(tbl.getParameters()));
      tbl.getParameters().put(hive_metastoreConstants.DDL_TIME,
          Long.toString(System.currentTimeMillis() / MILLISECOND_TO_SECOND_FACTOR));

      TableInput tableInput = GlueInputConverter.convertToTableInput(tbl);
      CreateTableRequest createTableRequest = new CreateTableRequest().withTableInput(tableInput)
          .withDatabaseName(tbl.getDbName()).withCatalogId(catalogId);
      glueClient.createTable(createTableRequest);
    } catch (AmazonServiceException e) {
      if (dirCreated) {
        Path tblPath = new Path(tbl.getSd().getLocation());
        wh.deleteDir(tblPath, true);
      }
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e){
      String msg = "Unable to create table: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  public boolean tableExists(String databaseName, String tableName) throws TException {
    checkArgument(StringUtils.isNotEmpty(databaseName), "databaseName cannot be null or empty");
    checkArgument(StringUtils.isNotEmpty(tableName), "tableName cannot be null or empty");

    if (!databaseExists(databaseName)) {
      throw new UnknownDBException("Database: " + databaseName + " does not exist.");
    }
    try {
      GetTableRequest getTableRequest = new GetTableRequest().withDatabaseName(databaseName).withName(tableName)
          .withCatalogId(catalogId);
      glueClient.getTable(getTableRequest);
      return true;
    } catch (EntityNotFoundException e) {
      return false;
    } catch (AmazonServiceException e){
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e){
      String msg = "Unable to check table exist: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  public org.apache.hadoop.hive.metastore.api.Table getTable(String dbName, String tableName) throws TException {
    checkArgument(StringUtils.isNotEmpty(dbName), "dbName cannot be null or empty");
    checkArgument(StringUtils.isNotEmpty(tableName), "tableName cannot be null or empty");

    try {
      GetTableRequest getTableRequest = new GetTableRequest().withDatabaseName(dbName).withName(tableName)
          .withCatalogId(catalogId);
      GetTableResult result = glueClient.getTable(getTableRequest);
      validateGlueTable(result.getTable());
      return CatalogToHiveConverter.convertTable(result.getTable(), dbName);
    } catch (AmazonServiceException e) {
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to get table: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  public List<String> getTables(String dbname, String tablePattern) throws TException {
    checkArgument(StringUtils.isNotEmpty(dbname), "dbName cannot be null or empty");

    List<String> names = Lists.newArrayList();
    try {
      String nextToken = null;

      do {
        GetTablesRequest getTablesRequest = new GetTablesRequest().withDatabaseName(dbname)
            .withExpression(tablePattern).withNextToken(nextToken).withCatalogId(catalogId);
        GetTablesResult result = glueClient.getTables(getTablesRequest);

        for (Table catalogTable : result.getTableList()) {
          names.add(catalogTable.getName());
        }

        nextToken = result.getNextToken();
      } while (nextToken != null);
      return names;
    } catch (AmazonServiceException e) {
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to get tables: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  public List<TableMeta> getTableMeta(
      String dbPatterns,
      String tablePatterns,
      List<String> tableTypes
  ) throws TException  {
    List<TableMeta> tables = new ArrayList<>();
    List<String> databases = getDatabases(dbPatterns);

    for (String dbName : databases) {
      String nextToken = null;

      do {
        GetTablesRequest getTablesRequest = new GetTablesRequest()
            .withDatabaseName(dbName)
            .withExpression(tablePatterns)
            .withNextToken(nextToken)
            .withCatalogId(catalogId);
        GetTablesResult result = glueClient.getTables(getTablesRequest);

        for (Table catalogTable : result.getTableList()) {
          if (tableTypes == null ||
              tableTypes.isEmpty() ||
              tableTypes.contains(catalogTable.getTableType())) {
            tables.add(CatalogToHiveConverter.convertTableMeta(catalogTable, dbName));
          }
        }

        nextToken = result.getNextToken();
      } while (nextToken != null);
    }

    return tables;
  }

  /*
   * Hive reference: https://github.com/apache/hive/blob/rel/release-2.3.0/metastore/src/java/org/apache/hadoop/hive/metastore/HiveAlterHandler.java#L88
   */
  public void alterTable(
      String dbName,
      String oldTableName,
      org.apache.hadoop.hive.metastore.api.Table newTable,
      EnvironmentContext environmentContext
  ) throws TException {
    checkArgument(StringUtils.isNotEmpty(dbName), "dbName cannot be null or empty");
    checkArgument(StringUtils.isNotEmpty(oldTableName), "oldTableName cannot be null or empty");
    checkNotNull(newTable, "newTable cannot be null");

    if (isCascade(environmentContext)) {
      throw new UnsupportedOperationException("Cascade for alter_table is not supported");
    }

    if (!oldTableName.equals(newTable.getTableName())) {
      throw new UnsupportedOperationException("Table rename is not supported");
    }

    validateTableObject(newTable, conf);
    if (!tableExists(dbName, oldTableName)) {
      throw new UnknownTableException("Table: " + oldTableName + " does not exists");
    }

    // If table properties has EXTERNAL set, update table type accordinly
    // mimics Hive's ObjectStore#convertToMTable, added in HIVE-1329
    boolean isExternal = Boolean.parseBoolean(newTable.getParameters().get("EXTERNAL"));
    if (MANAGED_TABLE.toString().equals(newTable.getTableType()) && isExternal) {
      newTable.setTableType(EXTERNAL_TABLE.toString());
    } else if (EXTERNAL_TABLE.toString().equals(newTable.getTableType()) && !isExternal) {
      newTable.setTableType(MANAGED_TABLE.toString());
    }

    if (hiveShims.requireCalStats(conf, null, null, newTable, environmentContext) && newTable.getPartitionKeys().isEmpty()) {
      //update table stats for non-partition Table
      org.apache.hadoop.hive.metastore.api.Database db = getDatabase(newTable.getDbName());
      hiveShims.updateTableStatsFast(db, newTable, wh, false, true, environmentContext);
    }

    try {
      TableInput newTableInput = GlueInputConverter.convertToTableInput(newTable);
      UpdateTableRequest updateTableRequest = new UpdateTableRequest().withDatabaseName(dbName)
          .withTableInput(newTableInput).withCatalogId(catalogId);
      glueClient.updateTable(updateTableRequest);
    } catch (AmazonServiceException e) {
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to alter table: " + oldTableName;
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  private boolean isCascade(EnvironmentContext environmentContext) {
    return environmentContext != null &&
        environmentContext.isSetProperties() &&
        StatsSetupConst.TRUE.equals(environmentContext.getProperties().get(StatsSetupConst.CASCADE));
  }

  public void dropTable(
      String dbName,
      String tableName,
      boolean deleteData,
      boolean ignoreUnknownTbl,
      boolean ifPurge
  ) throws TException {
    checkArgument(StringUtils.isNotEmpty(dbName), "dbName cannot be null or empty");
    checkArgument(StringUtils.isNotEmpty(tableName), "tableName cannot be null or empty");

    if (!tableExists(dbName, tableName)) {
      if (!ignoreUnknownTbl) {
        throw new UnknownTableException("Cannot find table: " + dbName + "." + tableName);
      } else {
        return;
      }
    }

    org.apache.hadoop.hive.metastore.api.Table tbl = getTable(dbName, tableName);
    String tblLocation = tbl.getSd().getLocation();
    boolean isExternal = isExternalTable(tbl);
    dropPartitionsForTable(dbName, tableName, deleteData && !isExternal);
    dropIndexesForTable(dbName, tableName, deleteData && !isExternal);

    try {
      DeleteTableRequest deleteTableRequest = new DeleteTableRequest().withDatabaseName(dbName).withName(tableName)
          .withCatalogId(catalogId);
      glueClient.deleteTable(deleteTableRequest);
    } catch (AmazonServiceException e){
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e){
      String msg = "Unable to drop table: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }

    if (StringUtils.isNotEmpty(tblLocation) && deleteData && !isExternal) {
      Path tblPath = new Path(tblLocation);
      try {
        wh.deleteDir(tblPath, true, ifPurge);
      } catch (Exception e){
        logger.error("Unable to remove table directory " + tblPath, e);
      }
    }
  }

  private void dropPartitionsForTable(String dbName, String tableName, boolean deleteData) throws TException {
    List<org.apache.hadoop.hive.metastore.api.Partition> partitionsToDelete = getPartitions(dbName, tableName, null, NO_MAX);
    for (org.apache.hadoop.hive.metastore.api.Partition part : partitionsToDelete) {
      dropPartition(dbName, tableName, part.getValues(), true, deleteData, false);
    }
  }

  private void dropIndexesForTable(String dbName, String tableName, boolean deleteData) throws TException {
    List<Index> indexesToDelete = listIndexes(dbName, tableName);
    for (Index index : indexesToDelete) {
      dropTable(dbName, index.getIndexTableName(), deleteData, true, false);
    }
  }

  public List<String> getTables(String dbname, String tablePattern, TableType tableType) throws TException {
    throw new UnsupportedOperationException("getTables with TableType is not supported");
  }

  public List<String> listTableNamesByFilter(String dbName, String filter, short maxTables) throws TException {
    throw new UnsupportedOperationException("listTableNamesByFilter is not supported");
  }

  /**
   * @return boolean
   *    true  -> directory created
   *    false -> directory not created
   */
  public boolean validateNewTableAndCreateDirectory(org.apache.hadoop.hive.metastore.api.Table tbl) throws TException {
    checkNotNull(tbl, "tbl cannot be null");
    if (tableExists(tbl.getDbName(), tbl.getTableName())) {
      throw new AlreadyExistsException("Table " + tbl.getTableName() + " already exists.");
    }
    validateTableObject(tbl, conf);

    if (TableType.VIRTUAL_VIEW.toString().equals(tbl.getTableType())) {
      // we don't need to create directory for virtual views
      return false;
    }

    if (StringUtils.isEmpty(tbl.getSd().getLocation())) {
      org.apache.hadoop.hive.metastore.api.Database db = getDatabase(tbl.getDbName());
      tbl.getSd().setLocation(hiveShims.getDefaultTablePath(db, tbl.getTableName(), wh).toString());
    } else {
      tbl.getSd().setLocation(wh.getDnsPath(new Path(tbl.getSd().getLocation())).toString());
    }

    Path tblPath = new Path(tbl.getSd().getLocation());
    return makeDirs(wh, tblPath);
  }

  // =========================== Partition ===========================

  public org.apache.hadoop.hive.metastore.api.Partition appendPartition(
      String dbName,
      String tblName,
      List<String> values
  ) throws TException {
    checkArgument(StringUtils.isNotEmpty(dbName), "dbName cannot be null or empty");
    checkArgument(StringUtils.isNotEmpty(tblName), "tblName cannot be null or empty");
    checkNotNull(values, "partition values cannot be null");
    org.apache.hadoop.hive.metastore.api.Table table = getTable(dbName, tblName);
    checkNotNull(table.getSd(), "StorageDescriptor cannot be null for Table " + tblName);
    org.apache.hadoop.hive.metastore.api.Partition partition = buildPartitionFromValues(table, values);
    addPartitions(Lists.newArrayList(partition), false, true);
    return partition;
  }

  /**
   * Taken from HiveMetaStore#append_partition_common
   */
  private org.apache.hadoop.hive.metastore.api.Partition buildPartitionFromValues(
    org.apache.hadoop.hive.metastore.api.Table table, List<String> values) throws MetaException {
    org.apache.hadoop.hive.metastore.api.Partition partition = new org.apache.hadoop.hive.metastore.api.Partition();
    partition.setDbName(table.getDbName());
    partition.setTableName(table.getTableName());
    partition.setValues(values);
    partition.setSd(table.getSd().deepCopy());

    Path partLocation = new Path(table.getSd().getLocation(), Warehouse.makePartName(table.getPartitionKeys(), values));
    partition.getSd().setLocation(partLocation.toString());

    long timeInSecond = System.currentTimeMillis() / MILLISECOND_TO_SECOND_FACTOR;
    partition.setCreateTime((int) timeInSecond);
    partition.putToParameters(hive_metastoreConstants.DDL_TIME, Long.toString(timeInSecond));
    return partition;
  }

  public List<org.apache.hadoop.hive.metastore.api.Partition> addPartitions(
      List<org.apache.hadoop.hive.metastore.api.Partition> partitions,
      boolean ifNotExists,
      boolean needResult
  ) throws TException {
    checkNotNull(partitions, "partitions cannot be null");
    List<Partition> partitionsCreated = batchCreatePartitions(partitions, ifNotExists);
    if (!needResult) {
      return null;
    }
    return CatalogToHiveConverter.convertPartitions(partitionsCreated);
  }

  private List<Partition> batchCreatePartitions(
      final List<org.apache.hadoop.hive.metastore.api.Partition> hivePartitions,
      final boolean ifNotExists
  ) throws TException {
    if (hivePartitions.isEmpty()) {
      return Lists.newArrayList();
    }

    final String dbName = hivePartitions.get(0).getDbName();
    final String tableName = hivePartitions.get(0).getTableName();
    org.apache.hadoop.hive.metastore.api.Table tbl = getTable(dbName, tableName);
    validateInputForBatchCreatePartitions(tbl, hivePartitions);

    List<Partition> catalogPartitions = Lists.newArrayList();
    Map<PartitionKey, Path> addedPath = Maps.newHashMap();
    try {
      for (org.apache.hadoop.hive.metastore.api.Partition partition : hivePartitions) {
        Path location = getPartitionLocation(tbl, partition);
        boolean partDirCreated = false;
        if (location != null) {
          partition.getSd().setLocation(location.toString());
          partDirCreated = makeDirs(wh, location);
        }
        Partition catalogPartition = HiveToCatalogConverter.convertPartition(partition);
        catalogPartitions.add(catalogPartition);
        if (partDirCreated) {
          addedPath.put(new PartitionKey(catalogPartition), new Path(partition.getSd().getLocation()));
        }
      }
    } catch (MetaException e) {
      for (Path path : addedPath.values()) {
        deletePath(path);
      }
      throw e;
    }

    List<Future<BatchCreatePartitionsHelper>> batchCreatePartitionsFutures = Lists.newArrayList();
    for (int i = 0; i < catalogPartitions.size(); i += BATCH_CREATE_PARTITIONS_MAX_REQUEST_SIZE) {
      int j = Math.min(i + BATCH_CREATE_PARTITIONS_MAX_REQUEST_SIZE, catalogPartitions.size());
      final List<Partition> partitionsOnePage = catalogPartitions.subList(i, j);

      batchCreatePartitionsFutures.add(this.executorService.submit(new Callable<BatchCreatePartitionsHelper>() {
        @Override
        public BatchCreatePartitionsHelper call() throws Exception {
          return new BatchCreatePartitionsHelper(glueClient, dbName, tableName, catalogId, partitionsOnePage, ifNotExists)
            .createPartitions();
        }
      }));
    }

    TException tException = null;
    List<Partition> partitionsCreated = Lists.newArrayList();
    for (Future<BatchCreatePartitionsHelper> future : batchCreatePartitionsFutures) {
      try {
        BatchCreatePartitionsHelper batchCreatePartitionsHelper = future.get();
        partitionsCreated.addAll(batchCreatePartitionsHelper.getPartitionsCreated());
        tException = tException == null ? batchCreatePartitionsHelper.getFirstTException() : tException;
        deletePathForPartitions(batchCreatePartitionsHelper.getPartitionsFailed(), addedPath);
      } catch (Exception e) {
        logger.error("Exception thrown by BatchCreatePartitions thread pool. ", e);
      }
    }

    if (tException != null) {
      throw tException;
    }
    return partitionsCreated;
  }

  private void validateInputForBatchCreatePartitions(
      org.apache.hadoop.hive.metastore.api.Table tbl,
      List<org.apache.hadoop.hive.metastore.api.Partition> hivePartitions) {
    checkNotNull(tbl.getPartitionKeys(), "Partition keys cannot be null");
    for (org.apache.hadoop.hive.metastore.api.Partition partition : hivePartitions) {
      checkArgument(tbl.getDbName().equals(partition.getDbName()), "Partitions must be in the same DB");
      checkArgument(tbl.getTableName().equals(partition.getTableName()), "Partitions must be in the same table");
      checkNotNull(partition.getValues(), "Partition values cannot be null");
      checkArgument(tbl.getPartitionKeys().size() == partition.getValues().size(), "Number of table partition keys must match number of partition values");
    }
  }

  private void deletePathForPartitions(List<Partition> partitions, Map<PartitionKey, Path> addedPath) {
    for (Partition partition : partitions) {
      Path path = addedPath.get(new PartitionKey(partition));
      if (path != null) {
        deletePath(path);
      }
    }
  }

  private void deletePath(Path path) {
    try {
      wh.deleteDir(path, true);
    } catch (MetaException e) {
      logger.error("Warehouse delete directory failed. ", e);
    }
  }

  /**
   * Taken from HiveMetastore#createLocationForAddedPartition
   */
  private Path getPartitionLocation(
      org.apache.hadoop.hive.metastore.api.Table tbl,
      org.apache.hadoop.hive.metastore.api.Partition part) throws MetaException {
    Path partLocation = null;
    String partLocationStr = null;
    if (part.getSd() != null) {
      partLocationStr = part.getSd().getLocation();
    }

    if (StringUtils.isEmpty(partLocationStr)) {
      // set default location if not specified and this is
      // a physical table partition (not a view)
      if (tbl.getSd().getLocation() != null) {
        partLocation = new Path(tbl.getSd().getLocation(),
            Warehouse.makePartName(tbl.getPartitionKeys(), part.getValues()));
      }
    } else {
      if (tbl.getSd().getLocation() == null) {
        throw new MetaException("Cannot specify location for a view partition");
      }
      partLocation = wh.getDnsPath(new Path(partLocationStr));
    }
    return partLocation;
  }

  public List<String> listPartitionNames(
      String databaseName,
      String tableName,
      List<String> values,
      short max
  ) throws TException {
    String expression = null;
    org.apache.hadoop.hive.metastore.api.Table table = getTable(databaseName, tableName);
    if (values != null) {
      expression = ExpressionHelper.buildExpressionFromPartialSpecification(table, values);
    }

    List<String> names = Lists.newArrayList();
    List<org.apache.hadoop.hive.metastore.api.Partition> partitions = getPartitions(databaseName, tableName, expression, max);
    for(org.apache.hadoop.hive.metastore.api.Partition p : partitions) {
      names.add(Warehouse.makePartName(table.getPartitionKeys(), p.getValues()));
    }
    return names;
  }

  public List<org.apache.hadoop.hive.metastore.api.Partition> getPartitionsByNames(
      String databaseName,
      String tableName,
      List<String> partitionNames
  ) throws TException {
    checkArgument(StringUtils.isNotEmpty(databaseName), "databaseName cannot be null or empty");
    checkArgument(StringUtils.isNotEmpty(tableName), "tableName cannot be null or empty");
    checkNotNull(partitionNames, "partitionNames cannot be null");

    List<PartitionValueList> partitionsToGet = Lists.newArrayList();
    for (String partitionName : partitionNames) {
      partitionsToGet.add(new PartitionValueList().withValues(partitionNameToVals(partitionName)));
    }

    List<List<PartitionValueList>> batchedPartitionsToGet = Lists.partition(partitionsToGet, BATCH_GET_PARTITIONS_MAX_REQUEST_SIZE);
    List<Future<BatchGetPartitionResult>> batchGetPartitionFutures = Lists.newArrayList();

    for (List<PartitionValueList> batch : batchedPartitionsToGet) {
      final BatchGetPartitionRequest request = new BatchGetPartitionRequest()
          .withDatabaseName(databaseName)
          .withTableName(tableName)
          .withPartitionsToGet(batch)
          .withCatalogId(catalogId);
      batchGetPartitionFutures.add(this.executorService.submit(new Callable<BatchGetPartitionResult>() {
        @Override
        public BatchGetPartitionResult call() throws Exception {
          return glueClient.batchGetPartition(request);
        }
      }));
    }

    List<org.apache.hadoop.hive.metastore.api.Partition> result = Lists.newArrayList();
    try {
      for (Future<BatchGetPartitionResult> future : batchGetPartitionFutures) {
        result.addAll(CatalogToHiveConverter.convertPartitions(future.get().getPartitions()));
      }
    } catch (ExecutionException e) {
      throw CatalogToHiveConverter.wrapInHiveException(e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return result;
  }

  public org.apache.hadoop.hive.metastore.api.Partition getPartition(String dbName, String tblName, String partitionName)
      throws TException {
    checkArgument(StringUtils.isNotEmpty(dbName), "dbName cannot be null or empty");
    checkArgument(StringUtils.isNotEmpty(tblName), "tblName cannot be null or empty");
    checkArgument(StringUtils.isNotEmpty(partitionName), "partitionName cannot be null or empty");
    List<String> values = partitionNameToVals(partitionName);
    return getPartition(dbName, tblName, values);
  }

  public org.apache.hadoop.hive.metastore.api.Partition getPartition(String dbName, String tblName, List<String> values) throws TException {
    checkArgument(StringUtils.isNotEmpty(dbName), "dbName cannot be null or empty");
    checkArgument(StringUtils.isNotEmpty(tblName), "tblName cannot be null or empty");
    checkNotNull(values, "values cannot be null");
   
    GetPartitionRequest request = new GetPartitionRequest()
        .withDatabaseName(dbName)
        .withTableName(tblName)
        .withPartitionValues(values)
        .withCatalogId(catalogId);
    Partition partition;
    try {
      GetPartitionResult res = glueClient.getPartition(request);
      partition = res.getPartition();
      if (partition == null) {
        logger.debug("No partitions were return for dbName = " + dbName + ", tblName = " + tblName + ", values = " + values);
        return null;
      }
    } catch (AmazonServiceException e) {
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to get partition with values: " + StringUtils.join(values, "/");
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
    return CatalogToHiveConverter.convertPartition(partition);
  }

  public List<org.apache.hadoop.hive.metastore.api.Partition> getPartitions(
      String databaseName,
      String tableName,
      String filter,
      long max
  ) throws TException {
    checkArgument(StringUtils.isNotEmpty(databaseName), "databaseName cannot be null or empty");
    checkArgument(StringUtils.isNotEmpty(tableName), "tableName cannot be null or empty");
    List<Partition> partitions = getCatalogPartitions(databaseName, tableName, filter, max);
    return CatalogToHiveConverter.convertPartitions(partitions);
  }

  public List<Partition> getCatalogPartitions(
      final String databaseName,
      final String tableName,
      final String expression,
      final long max
  ) throws TException {
    checkArgument(StringUtils.isNotEmpty(databaseName), "databaseName cannot be null or empty");
    checkArgument(StringUtils.isNotEmpty(tableName), "tableName cannot be null or empty");
    if (max == 0) {
      return Collections.emptyList();
    }

    if (max < 0 || max > GET_PARTITIONS_MAX_SIZE) {
      return getPartitionsParallel(databaseName, tableName, expression, max);
    } else {
      // We don't need to get too many partitions, so just do it serially.
      return getCatalogPartitions(databaseName, tableName, expression, max, null);
    }
  }

  private List<Partition> getPartitionsParallel(
          final String databaseName,
          final String tableName,
          final String expression,
          final long max) throws TException {
    // Prepare the segments
    List<Segment> segments = Lists.newArrayList();
    for (int i = 0; i < numPartitionSegments; i++) {
      segments.add(new Segment()
              .withSegmentNumber(i)
              .withTotalSegments(numPartitionSegments));
    }
    // Submit Glue API calls in parallel using the thread pool.
    // We could convert this into a parallelStream after upgrading to JDK 8 compiler base.
    List<Future<List<Partition>>> futures = Lists.newArrayList();
    for (final Segment segment : segments) {
      futures.add(this.executorService.submit(new Callable<List<Partition>>() {
        @Override
        public List<Partition> call() throws Exception {
          return getCatalogPartitions(databaseName, tableName, expression, max, segment);
        }
      }));
    }

    // Get the results
    List<Partition> partitions = Lists.newArrayList();
    try {
      for (Future<List<Partition>> future : futures) {
        List<Partition> segmentPartitions = future.get();
        if (partitions.size() + segmentPartitions.size() >= max && max > 0) {
          // Extract the required number of partitions from the segment and we're done.
          long remaining = max - partitions.size();
          partitions.addAll(segmentPartitions.subList(0, (int) remaining));
          break;
        } else {
          partitions.addAll(segmentPartitions);
        }
      }
    } catch (ExecutionException e) {
      Throwables.propagateIfInstanceOf(e.getCause(), TException.class);
      Throwables.propagate(e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return partitions;
  }

  private List<Partition> getCatalogPartitions(
          String databaseName,
          String tableName,
          String expression,
          long max,
          Segment segment) throws TException {
    List<Partition> partitions = Lists.newArrayList();
    String nextToken = null;
    do {
      GetPartitionsRequest request = new GetPartitionsRequest()
          .withDatabaseName(databaseName)
          .withTableName(tableName)
          .withExpression(expression)
          .withNextToken(nextToken)
          .withCatalogId(catalogId)
          .withSegment(segment);
      try {
        GetPartitionsResult res = glueClient.getPartitions(request);
        List<Partition> list = res.getPartitions();
        if ((partitions.size() + list.size()) >= max && max > 0) {
          long remaining = max - partitions.size();
          partitions.addAll(list.subList(0, (int) remaining));
          break;
        }
        partitions.addAll(list);
        nextToken = res.getNextToken();
      } catch (AmazonServiceException e) {
        throw CatalogToHiveConverter.wrapInHiveException(e);
      } catch (Exception e) {
        String msg = "Unable to get partitions with expression: " + expression;
        logger.error(msg, e);
        throw new MetaException(msg + e);
      }
    } while (nextToken != null);

    return partitions;
  }

  public boolean dropPartition(
      String dbName,
      String tblName,
      List<String>values,
      boolean ifExist,
      boolean deleteData,
      boolean purgeData
  ) throws TException {
    checkArgument(StringUtils.isNotEmpty(dbName), "dbName cannot be null or empty");
    checkArgument(StringUtils.isNotEmpty(tblName), "tblName cannot be null or empty");
    checkNotNull(values, "values cannot be null");

    org.apache.hadoop.hive.metastore.api.Partition partition = null;
    try {
      partition = getPartition(dbName, tblName, values);
    } catch (NoSuchObjectException e) {
      if (ifExist) {
        return true;
      }
    }

    try {
      DeletePartitionRequest request = new DeletePartitionRequest()
          .withDatabaseName(dbName)
          .withTableName(tblName)
          .withPartitionValues(values)
          .withCatalogId(catalogId);
      glueClient.deletePartition(request);
    } catch (AmazonServiceException e) {
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to drop partition with values: " + StringUtils.join(values, "/");
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }

    performDropPartitionPostProcessing(dbName, tblName, partition, deleteData, purgeData);
    return true;
  }

  private void performDropPartitionPostProcessing(
      String dbName,
      String tblName,
      org.apache.hadoop.hive.metastore.api.Partition partition,
      boolean deleteData,
      boolean ifPurge
  ) throws TException {
    if (deleteData && partition.getSd() != null && partition.getSd().getLocation() != null) {
      Path partPath = new Path(partition.getSd().getLocation());
      org.apache.hadoop.hive.metastore.api.Table table = getTable(dbName, tblName);
      if (isExternalTable(table)) {
        //Don't delete external table data
        return;
      }
      boolean mustPurge = isMustPurge(table, ifPurge);
      wh.deleteDir(partPath, true, mustPurge);
      try {
        List<String> values = partition.getValues();
        deleteParentRecursive(partPath.getParent(), values.size() - 1, mustPurge);
      } catch (IOException e) {
        throw new MetaException(e.getMessage());
      }
    }
  }

  /**
   * Taken from HiveMetaStore#isMustPurge
   */
  private boolean isMustPurge(org.apache.hadoop.hive.metastore.api.Table table, boolean ifPurge) {
    return (ifPurge || "true".equalsIgnoreCase(table.getParameters().get("auto.purge")));
  }

  /**
   * Taken from HiveMetaStore#deleteParentRecursive
   */
  private void deleteParentRecursive(Path parent, int depth, boolean mustPurge) throws IOException, MetaException {
    if (depth > 0 && parent != null && wh.isWritable(parent) && wh.isEmpty(parent)) {
      wh.deleteDir(parent, true, mustPurge);
      deleteParentRecursive(parent.getParent(), depth - 1, mustPurge);
    }
  }

  public void alterPartitions(
    String dbName,
    String tblName,
    List<org.apache.hadoop.hive.metastore.api.Partition> partitions
  ) throws TException {
    checkArgument(StringUtils.isNotEmpty(dbName), "dbName cannot be null or empty");
    checkArgument(StringUtils.isNotEmpty(tblName), "tblName cannot be null or empty");
    checkNotNull(partitions, "partitions cannot be null");

    for (org.apache.hadoop.hive.metastore.api.Partition part : partitions) {
      part.setParameters(deepCopyMap(part.getParameters()));
      if (part.getParameters().get(hive_metastoreConstants.DDL_TIME) == null ||
          Integer.parseInt(part.getParameters().get(hive_metastoreConstants.DDL_TIME)) == 0) {
        part.putToParameters(hive_metastoreConstants.DDL_TIME, Long.toString(System.currentTimeMillis() / MILLISECOND_TO_SECOND_FACTOR));
      }

      PartitionInput partitionInput = GlueInputConverter.convertToPartitionInput(part);
      UpdatePartitionRequest request = new UpdatePartitionRequest()
        .withDatabaseName(dbName)
        .withTableName(tblName)
        .withPartitionInput(partitionInput)
        .withPartitionValueList(part.getValues())
        .withCatalogId(catalogId);
     
      try {
        glueClient.updatePartition(request);
      } catch (AmazonServiceException e) {
        throw CatalogToHiveConverter.wrapInHiveException(e);
      } catch (Exception e) {
        String msg = "Unable to alter partition: ";
        logger.error(msg, e);
        throw new MetaException(msg + e);
      }
    }
  }

  /**
   *  Taken from HiveMetaStore#partition_name_to_vals
   */
  public List<String> partitionNameToVals(String name) throws TException {
    checkNotNull(name, "name cannot be null");
    if (name.isEmpty()) {
      return Lists.newArrayList();
    }
    LinkedHashMap<String, String> map = Warehouse.makeSpecFromName(name);
    List<String> vals = Lists.newArrayList();
    vals.addAll(map.values());
    return vals;
  }

  // ============================ Index ==============================

  public List<Index> listIndexes(String dbName, String tblName) throws TException {
    checkArgument(StringUtils.isNotEmpty(dbName), "dbName cannot be null or empty");
    checkArgument(StringUtils.isNotEmpty(tblName), "tblName cannot be null or empty");

    org.apache.hadoop.hive.metastore.api.Table originTable = getTable(dbName, tblName);
    Map<String, String> parameters = originTable.getParameters();
    List<Table> indexTableObjects = Lists.newArrayList();
    for(String key : parameters.keySet()) {
      if(key.startsWith(INDEX_PREFIX)) {
        String serialisedString = parameters.get(key);
        indexTableObjects.add(stringToCatalogTable(serialisedString));
      }
    }

    List<Index> hiveIndexList = Lists.newArrayList();
    for (Table catalogIndexTableObject : indexTableObjects) {
      hiveIndexList.add(CatalogToHiveConverter.convertTableObjectToIndex(catalogIndexTableObject));
    }
    return hiveIndexList;
  }

  // ======================= Roles & Privilege =======================

  public boolean createRole(org.apache.hadoop.hive.metastore.api.Role role) throws TException {
    throw new UnsupportedOperationException("createRole is not supported");
  }

  public boolean dropRole(String roleName) throws TException {
    throw new UnsupportedOperationException("dropRole is not supported");
  }

  public List<org.apache.hadoop.hive.metastore.api.Role> listRoles(
      String principalName,
      org.apache.hadoop.hive.metastore.api.PrincipalType principalType
  ) throws TException {
    // All users belong to public role implicitly, add that role
    // Bring logic from Hive's ObjectStore
    // https://code.amazon.com/packages/Aws157Hive/blobs/48f6e30080df475ffe54c39f70dd134268e30358/
    // --/metastore/src/java/org/apache/hadoop/hive/metastore/ObjectStore.java#L4208
    if (principalType == PrincipalType.USER) {
      return implicitRoles;
    } else {
      throw new UnsupportedOperationException(
          "listRoles is only supported for " + PrincipalType.USER + " Principal type");
    }
  }

  public List<String> listRoleNames() throws TException {
    // return PUBLIC role as implicit role to prevent unnecessary failure,
    // even though Glue doesn't support Role API yet
    // https://code.amazon.com/packages/Aws157Hive/blobs/48f6e30080df475ffe54c39f70dd134268e30358/
    // --/metastore/src/java/org/apache/hadoop/hive/metastore/ObjectStore.java#L4325
    return Lists.newArrayList(PUBLIC);
  }

  public org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse getPrincipalsInRole(
      org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest request
  ) throws TException {
    throw new UnsupportedOperationException("getPrincipalsInRole is not supported");
  }

  public GetRoleGrantsForPrincipalResponse getRoleGrantsForPrincipal(
      GetRoleGrantsForPrincipalRequest request
  ) throws TException {
    throw new UnsupportedOperationException("getRoleGrantsForPrincipal is not supported");
  }

  public boolean grantRole(
      String roleName,
      String userName,
      org.apache.hadoop.hive.metastore.api.PrincipalType principalType,
      String grantor, org.apache.hadoop.hive.metastore.api.PrincipalType grantorType,
      boolean grantOption
  ) throws TException {
    throw new UnsupportedOperationException("grantRole is not supported");
  }

  public boolean revokeRole(
      String roleName,
      String userName,
      org.apache.hadoop.hive.metastore.api.PrincipalType principalType,
      boolean grantOption
  ) throws TException {
    throw new UnsupportedOperationException("revokeRole is not supported");
  }

  public boolean revokePrivileges(
      org.apache.hadoop.hive.metastore.api.PrivilegeBag privileges,
      boolean grantOption
  ) throws TException {
    throw new UnsupportedOperationException("revokePrivileges is not supported");
  }

  public boolean grantPrivileges(org.apache.hadoop.hive.metastore.api.PrivilegeBag privileges)
      throws TException {
    throw new UnsupportedOperationException("grantPrivileges is not supported");
  }

  public org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet getPrivilegeSet(
    HiveObjectRef objectRef,
    String user, List<String> groups
  ) throws TException {
    // getPrivilegeSet is NOT yet supported.
    // return null not to break due to optional info
    // Hive return null when every condition fail
    // https://code.amazon.com/packages/Aws157Hive/blobs/c1ced60e67765d27086b3621255cd843947c151e/
    // --/metastore/src/java/org/apache/hadoop/hive/metastore/HiveMetaStore.java#L5237
    return null;
  }

  public List<HiveObjectPrivilege> listPrivileges(
    String principal,
    org.apache.hadoop.hive.metastore.api.PrincipalType principalType,
    HiveObjectRef objectRef
  ) throws TException {
    throw new UnsupportedOperationException("listPrivileges is not supported");
  }

  // ========================== Statistics ==========================

  public boolean deletePartitionColumnStatistics(
    String dbName,
    String tableName,
    String partName,
    String colName
  ) throws TException {
    throw new UnsupportedOperationException("deletePartitionColumnStatistics is not supported");
  }

  public boolean deleteTableColumnStatistics(
    String dbName,
    String tableName,
    String colName
  ) throws TException {
    throw new UnsupportedOperationException("deleteTableColumnStatistics is not supported");
  }

  public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
    String dbName,
    String tableName,
    List<String> partitionNames, List<String> columnNames
  ) throws TException {
    throw new UnsupportedOperationException("getPartitionColumnStatistics is not supported");
  }

  public List<ColumnStatisticsObj> getTableColumnStatistics(
    String dbName,
    String tableName,
    List<String> colNames
  ) throws TException {
    throw new UnsupportedOperationException("getTableColumnStatistics is not supported");
  }

  public boolean updatePartitionColumnStatistics(
    org.apache.hadoop.hive.metastore.api.ColumnStatistics columnStatistics
  ) throws TException {
    throw new UnsupportedOperationException("updatePartitionColumnStatistics is not supported");
  }

  public boolean updateTableColumnStatistics(
    org.apache.hadoop.hive.metastore.api.ColumnStatistics columnStatistics
  ) throws TException {
    throw new UnsupportedOperationException("updateTableColumnStatistics is not supported");
  }

  public AggrStats getAggrColStatsFor(
    String dbName,
    String tblName,
    List<String> colNames,
    List<String> partName
  ) throws TException {
    throw new UnsupportedOperationException("getAggrColStatsFor is not supported");
  }

  public void cancelDelegationToken(String tokenStrForm) throws TException {
    throw new UnsupportedOperationException("cancelDelegationToken is not supported");
  }

  public String getTokenStrForm() throws IOException {
    throw new UnsupportedOperationException("getTokenStrForm is not supported");
  }

  public boolean addToken(String tokenIdentifier, String delegationToken) throws TException {
    throw new UnsupportedOperationException("addToken is not supported");
  }

  public boolean removeToken(String tokenIdentifier) throws TException {
    throw new UnsupportedOperationException("removeToken is not supported");
  }

  public String getToken(String tokenIdentifier) throws TException {
    throw new UnsupportedOperationException("getToken is not supported");
  }

  public List<String> getAllTokenIdentifiers() throws TException {
    throw new UnsupportedOperationException("getAllTokenIdentifiers is not supported");
  }

  public int addMasterKey(String key) throws TException {
    throw new UnsupportedOperationException("addMasterKey is not supported");
  }

  public void updateMasterKey(Integer seqNo, String key) throws TException {
    throw new UnsupportedOperationException("updateMasterKey is not supported");
  }

  public boolean removeMasterKey(Integer keySeq) throws TException {
    throw new UnsupportedOperationException("removeMasterKey is not supported");
  }

  public String[] getMasterKeys() throws TException {
    throw new UnsupportedOperationException("getMasterKeys is not supported");
  }

  public LockResponse checkLock(long lockId) throws TException {
    throw new UnsupportedOperationException("checkLock is not supported");
  }

  public void commitTxn(long txnId) throws TException {
    throw new UnsupportedOperationException("commitTxn is not supported");
  }

  public void abortTxns(List<Long> txnIds) throws TException {
    throw new UnsupportedOperationException("abortTxns is not supported");
  }

  public void compact(
      String dbName,
      String tblName,
      String partitionName,
      CompactionType compactionType
  ) throws TException {
    throw new UnsupportedOperationException("compact is not supported");
  }

  public void compact(
      String dbName,
      String tblName,
      String partitionName,
      CompactionType compactionType,
      Map<String, String> tblProperties
  ) throws TException {
    throw new UnsupportedOperationException("compact is not supported");
  }

  public CompactionResponse compact2(
      String dbName,
      String tblName,
      String partitionName,
      CompactionType compactionType,
      Map<String, String> tblProperties
  ) throws TException {
    throw new UnsupportedOperationException("compact2 is not supported");
  }

  public ValidTxnList getValidTxns() throws TException {
    throw new UnsupportedOperationException("getValidTxns is not supported");
  }

  public ValidTxnList getValidTxns(long currentTxn) throws TException {
    throw new UnsupportedOperationException("getValidTxns is not supported");
  }

  public org.apache.hadoop.hive.metastore.api.Partition exchangePartition(
      Map<String, String> partitionSpecs,
      String srcDb,
      String srcTbl,
      String dstDb,
      String dstTbl
  ) throws TException {
    throw new UnsupportedOperationException("exchangePartition not yet supported.");
  }

  public List<org.apache.hadoop.hive.metastore.api.Partition> exchangePartitions(
      Map<String, String> partitionSpecs,
      String sourceDb,
      String sourceTbl,
      String destDb,
      String destTbl
  ) throws TException {
    throw new UnsupportedOperationException("exchangePartitions is not yet supported");
  }

  public String getDelegationToken(
      String owner,
      String renewerKerberosPrincipalName
  ) throws TException {
    throw new UnsupportedOperationException("getDelegationToken is not supported");
  }

  public void heartbeat(long txnId, long lockId) throws TException {
    throw new UnsupportedOperationException("heartbeat is not supported");
  }

  public HeartbeatTxnRangeResponse heartbeatTxnRange(long min, long max) throws TException {
    throw new UnsupportedOperationException("heartbeatTxnRange is not supported");
  }

  public boolean isPartitionMarkedForEvent(
      String dbName,
      String tblName,
      Map<String, String> partKVs,
      PartitionEventType eventType
  ) throws TException {
    throw new UnsupportedOperationException("isPartitionMarkedForEvent is not supported");
  }

  public int getNumPartitionsByFilter(
      String dbName,
      String tableName,
      String filter
  ) throws TException {
    throw new UnsupportedOperationException("getNumPartitionsByFilter is not supported.");
  }

  public PartitionSpecProxy listPartitionSpecs(
      String dbName,
      String tblName,
      int max
  ) throws TException {
    throw new UnsupportedOperationException("listPartitionSpecs is not supported.");
  }

  public PartitionSpecProxy listPartitionSpecsByFilter(
      String dbName,
      String tblName,
      String filter,
      int max
  ) throws TException {
    throw new UnsupportedOperationException("listPartitionSpecsByFilter is not supported");
  }

  public LockResponse lock(LockRequest lockRequest) throws TException {
    throw new UnsupportedOperationException("lock is not supported");
  }

  public void markPartitionForEvent(
      String dbName,
      String tblName,
      Map<String, String> partKeyValues,
      PartitionEventType eventType
  ) throws  TException {
    throw new UnsupportedOperationException("markPartitionForEvent is not supported");
  }

  public long openTxn(String user) throws TException {
    throw new UnsupportedOperationException("openTxn is not supported");
  }

  public OpenTxnsResponse openTxns(String user, int numTxns) throws TException {
    throw new UnsupportedOperationException("openTxns is not supported");
  }

  public long renewDelegationToken(String tokenStrForm) throws  TException {
    throw new UnsupportedOperationException("renewDelegationToken is not supported");
  }

  public void rollbackTxn(long txnId) throws TException {
    throw new UnsupportedOperationException("rollbackTxn is not supported");
  }

  public void createTableWithConstraints(
      org.apache.hadoop.hive.metastore.api.Table table,
      List<SQLPrimaryKey> primaryKeys,
      List<SQLForeignKey> foreignKeys
  ) throws AlreadyExistsException, TException {
    throw new UnsupportedOperationException("createTableWithConstraints is not supported");
  }

  public void dropConstraint(
      String dbName,
      String tblName,
      String constraintName
  ) throws TException {
    throw new UnsupportedOperationException("dropConstraint is not supported");
  }

  public void addPrimaryKey(List<SQLPrimaryKey> primaryKeyCols) throws TException {
    throw new UnsupportedOperationException("addPrimaryKey is not supported");
  }

  public void addForeignKey(List<SQLForeignKey> foreignKeyCols) throws TException {
    throw new UnsupportedOperationException("addForeignKey is not supported");
  }

  public ShowCompactResponse showCompactions() throws TException {
    throw new UnsupportedOperationException("showCompactions is not supported");
  }

  public void addDynamicPartitions(
      long txnId,
      String dbName,
      String tblName,
      List<String> partNames
  ) throws TException {
    throw new UnsupportedOperationException("addDynamicPartitions is not supported");
  }

  public void addDynamicPartitions(
      long txnId,
      String dbName,
      String tblName,
      List<String> partNames,
      DataOperationType operationType
  ) throws TException {
    throw new UnsupportedOperationException("addDynamicPartitions is not supported");
  }

  public void insertTable(org.apache.hadoop.hive.metastore.api.Table table, boolean overwrite) throws MetaException {
    throw new UnsupportedOperationException("insertTable is not supported");
  }

  public NotificationEventResponse getNextNotification(
      long lastEventId,
      int maxEvents,
      IMetaStoreClient.NotificationFilter notificationFilter
  ) throws TException {
    throw new UnsupportedOperationException("getNextNotification is not supported");
  }

  public CurrentNotificationEventId getCurrentNotificationEventId() throws TException {
    throw new UnsupportedOperationException("getCurrentNotificationEventId is not supported");
  }

  public FireEventResponse fireListenerEvent(FireEventRequest fireEventRequest) throws TException {
    throw new UnsupportedOperationException("fireListenerEvent is not supported");
  }

  public ShowLocksResponse showLocks() throws TException {
    throw new UnsupportedOperationException("showLocks is not supported");
  }

  public ShowLocksResponse showLocks(ShowLocksRequest showLocksRequest) throws TException {
    throw new UnsupportedOperationException("showLocks is not supported");
  }

  public GetOpenTxnsInfoResponse showTxns() throws TException {
    throw new UnsupportedOperationException("showTxns is not supported");
  }

  public void unlock(long lockId) throws TException {
    throw new UnsupportedOperationException("unlock is not supported");
  }

  public Iterable<Map.Entry<Long, ByteBuffer>> getFileMetadata(List<Long> fileIds) throws TException {
    throw new UnsupportedOperationException("getFileMetadata is not supported");
  }

  public Iterable<Map.Entry<Long, MetadataPpdResult>> getFileMetadataBySarg(
      List<Long> fileIds,
      ByteBuffer sarg,
      boolean doGetFooters
  ) throws TException {
    throw new UnsupportedOperationException("getFileMetadataBySarg is not supported");
  }

  public void clearFileMetadata(List<Long> fileIds) throws TException {
    throw new UnsupportedOperationException("clearFileMetadata is not supported");
  }

  public void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata) throws TException {
    throw new UnsupportedOperationException("putFileMetadata is not supported");
  }

  public boolean setPartitionColumnStatistics(
      org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest request
  ) throws TException {
    throw new UnsupportedOperationException("setPartitionColumnStatistics is not supported");
  }

  public boolean cacheFileMetadata(
      String dbName,
      String tblName,
      String partName,
      boolean allParts
  ) throws TException {
    throw new UnsupportedOperationException("cacheFileMetadata is not supported");
  }

  public int addPartitionsSpecProxy(PartitionSpecProxy pSpec) throws TException {
    throw new UnsupportedOperationException("addPartitionsSpecProxy is unsupported");
  }

  public void setUGI(String username) throws TException {
    throw new UnsupportedOperationException("setUGI is unsupported");
  }
  
  /**
   * Gets the user defined function in a database stored in metastore and
   * converts back to Hive function.
   * 
   * @param dbName
   * @param functionName
   * @return
   * @throws MetaException
   * @throws TException
   */
  public org.apache.hadoop.hive.metastore.api.Function getFunction(String dbName, String functionName)
      throws TException {
    try {
      GetUserDefinedFunctionRequest getUserDefinedFunctionRequest = new GetUserDefinedFunctionRequest()
          .withDatabaseName(dbName).withFunctionName(functionName).withCatalogId(catalogId);
      GetUserDefinedFunctionResult result = glueClient.getUserDefinedFunction(getUserDefinedFunctionRequest);
      return CatalogToHiveConverter.convertFunction(dbName, result.getUserDefinedFunction());
    } catch (AmazonServiceException e) {
      logger.error(e);
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to get Function: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  /**
   * Gets user defined functions that match a pattern in database stored in
   * metastore and converts back to Hive function.
   * 
   * @param dbName
   * @param functionName
   * @return
   * @throws MetaException
   * @throws TException
   */
  public List<String> getFunctions(String dbName, String pattern) throws TException {
    try {
      List<String> functionNames = Lists.newArrayList();
      String nextToken = null;

      do {
        GetUserDefinedFunctionsRequest getUserDefinedFunctionsRequest = new GetUserDefinedFunctionsRequest()
            .withDatabaseName(dbName).withPattern(pattern).withNextToken(nextToken).withCatalogId(catalogId);
        GetUserDefinedFunctionsResult result = glueClient.getUserDefinedFunctions(getUserDefinedFunctionsRequest);
        nextToken = result.getNextToken();

        for (UserDefinedFunction catalogFunction : result.getUserDefinedFunctions()) {
          functionNames.add(catalogFunction.getFunctionName());
        }
      } while (nextToken != null);
      return functionNames;
    } catch (AmazonServiceException e) {
      logger.error(e);
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to get Functions: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  /**
   * Creates a new user defined function in the metastore.
   * 
   * @param function
   * @throws InvalidObjectException
   * @throws MetaException
   * @throws TException
   */
  public void createFunction(org.apache.hadoop.hive.metastore.api.Function function) throws InvalidObjectException,
      TException {
    try {
      UserDefinedFunctionInput functionInput = GlueInputConverter.convertToUserDefinedFunctionInput(function);
      CreateUserDefinedFunctionRequest createUserDefinedFunctionRequest = new CreateUserDefinedFunctionRequest()
          .withDatabaseName(function.getDbName()).withFunctionInput(functionInput).withCatalogId(catalogId);
      glueClient.createUserDefinedFunction(createUserDefinedFunctionRequest);
    } catch (AmazonServiceException e) {
      logger.error(e);
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to create Function: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  /**
   * Drops a user defined function in the database stored in metastore.
   * 
   * @param dbName
   * @param functionName
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws InvalidObjectException
   * @throws org.apache.hadoop.hive.metastore.api.InvalidInputException
   * @throws TException
   */
  public void dropFunction(String dbName, String functionName) throws NoSuchObjectException,
      InvalidObjectException, org.apache.hadoop.hive.metastore.api.InvalidInputException, TException {
    try {
      DeleteUserDefinedFunctionRequest deleteUserDefinedFunctionRequest = new DeleteUserDefinedFunctionRequest()
          .withDatabaseName(dbName).withFunctionName(functionName).withCatalogId(catalogId);
      glueClient.deleteUserDefinedFunction(deleteUserDefinedFunctionRequest);
    } catch (AmazonServiceException e) {
      logger.error(e);
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to drop Function: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
  }
  
  /**
   * Updates a user defined function in a database stored in the metastore.
   * 
   * @param dbName
   * @param functionName
   * @param newFunction
   * @throws InvalidObjectException
   * @throws MetaException
   * @throws TException
   */
  public void alterFunction(String dbName, String functionName,
      org.apache.hadoop.hive.metastore.api.Function newFunction) throws InvalidObjectException, MetaException,
      TException {
    try {
      UserDefinedFunctionInput functionInput = GlueInputConverter.convertToUserDefinedFunctionInput(newFunction);
      UpdateUserDefinedFunctionRequest updateUserDefinedFunctionRequest = new UpdateUserDefinedFunctionRequest()
          .withDatabaseName(dbName).withFunctionName(functionName).withFunctionInput(functionInput)
          .withCatalogId(catalogId);
      glueClient.updateUserDefinedFunction(updateUserDefinedFunctionRequest);
    } catch (AmazonServiceException e) {
      logger.error(e);
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to alter Function: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
  }
  
  /**
   * Fetches the fields for a table in a database.
   * 
   * @param db
   * @param tableName
   * @return
   * @throws MetaException
   * @throws TException
   * @throws UnknownTableException
   * @throws UnknownDBException
   */
  public List<FieldSchema> getFields(String db, String tableName) throws MetaException, TException,
      UnknownTableException, UnknownDBException {
    try {
      GetTableRequest getTableRequest = new GetTableRequest().withDatabaseName(db).withName(tableName)
          .withCatalogId(catalogId);
      GetTableResult result = glueClient.getTable(getTableRequest);
      Table table = result.getTable();
      return CatalogToHiveConverter.convertFieldSchemaList(table.getStorageDescriptor().getColumns());
    } catch (AmazonServiceException e) {
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to get field from table: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
  }
  
  /**
   * Fetches the schema for a table in a database.
   * 
   * @param db
   * @param tableName
   * @return
   * @throws MetaException
   * @throws TException
   * @throws UnknownTableException
   * @throws UnknownDBException
   */
  public List<FieldSchema> getSchema(String db, String tableName) throws TException,
      UnknownTableException, UnknownDBException {
    try {
      GetTableRequest getTableRequest = new GetTableRequest().withDatabaseName(db).withName(tableName)
          .withCatalogId(catalogId);
      GetTableResult result = glueClient.getTable(getTableRequest);
      Table table = result.getTable();
      List<Column> schemas = table.getStorageDescriptor().getColumns();
      if (table.getPartitionKeys() != null && !table.getPartitionKeys().isEmpty()) {
        schemas.addAll(table.getPartitionKeys());
      }
      return CatalogToHiveConverter.convertFieldSchemaList(schemas);
    } catch (AmazonServiceException e) {
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to get field from table: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  /**
   * Updates the partition values for a table in database stored in metastore.
   * 
   * @param databaseName
   * @param tableName
   * @param partitionValues
   * @param newPartition
   * @throws InvalidOperationException
   * @throws MetaException
   * @throws TException
   */
  public void renamePartitionInCatalog(String databaseName, String tableName, List<String> partitionValues,
      org.apache.hadoop.hive.metastore.api.Partition newPartition) throws InvalidOperationException,
      TException {
    try {
      UpdatePartitionRequest updatePartitionRequest = new UpdatePartitionRequest().withDatabaseName(databaseName)
          .withTableName(tableName).withPartitionValueList(partitionValues)
          .withPartitionInput(GlueInputConverter.convertToPartitionInput(newPartition)).withCatalogId(catalogId);
      glueClient.updatePartition(updatePartitionRequest);
    } catch (AmazonServiceException e) {
      throw CatalogToHiveConverter.wrapInHiveException(e);
    }
  }
}
