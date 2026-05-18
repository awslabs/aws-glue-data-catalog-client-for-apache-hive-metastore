package com.amazonaws.glue.catalog.metastore;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import com.amazonaws.glue.catalog.converters.BaseCatalogToHiveConverter;
import com.amazonaws.glue.catalog.converters.CatalogToHiveConverter;
import com.amazonaws.glue.catalog.util.BatchDeletePartitionsHelper;
import com.amazonaws.glue.catalog.util.ExpressionHelper;
import com.amazonaws.glue.catalog.util.LoggingHelper;
import com.amazonaws.glue.catalog.util.MetastoreClientUtils;
import com.amazonaws.glue.shims.AwsGlueHiveShims;
import com.amazonaws.glue.shims.ShimsLoader;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.Partition;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.utils.ObjectPair;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import static com.amazonaws.glue.catalog.util.MetastoreClientUtils.isExternalTable;
import static com.google.common.base.Preconditions.checkNotNull;



public class AWSCatalogMetastoreClient implements IMetaStoreClient {

  // TODO "hook" into Hive logging (hive or hive.metastore)
  private static final Logger logger = Logger.getLogger(AWSCatalogMetastoreClient.class);

  private final HiveConf conf;
  private final GlueClient glueClient;
  private final Warehouse wh;
  private final GlueMetastoreClientDelegate glueMetastoreClientDelegate;
  private final String catalogId;
  private final CatalogToHiveConverter catalogToHiveConverter;

  private static final int BATCH_DELETE_PARTITIONS_PAGE_SIZE = 25;
  private static final int BATCH_DELETE_PARTITIONS_THREADS_COUNT = 5;
  static final String BATCH_DELETE_PARTITIONS_THREAD_POOL_NAME_FORMAT = "batch-delete-partitions-%d";
  private static final ExecutorService BATCH_DELETE_PARTITIONS_THREAD_POOL = Executors.newFixedThreadPool(
    BATCH_DELETE_PARTITIONS_THREADS_COUNT,
    new ThreadFactoryBuilder()
      .setNameFormat(BATCH_DELETE_PARTITIONS_THREAD_POOL_NAME_FORMAT)
      .setDaemon(true).build()
  );

  private final AwsGlueHiveShims hiveShims = ShimsLoader.getHiveShims();
  private Map<String, String> currentMetaVars;

  public AWSCatalogMetastoreClient(HiveConf conf, HiveMetaHookLoader hook) throws MetaException {
    this.conf = conf;
    catalogId = MetastoreClientUtils.getCatalogId(conf);
    glueClient = new AWSGlueClientFactory(this.conf).newClient();
    catalogToHiveConverter = new BaseCatalogToHiveConverter();

    // TODO preserve existing functionality for HiveMetaHook
    wh = new Warehouse(this.conf);

    AWSGlueMetastore glueMetastore = new AWSGlueMetastoreFactory().newMetastore(conf);
    glueMetastoreClientDelegate = new GlueMetastoreClientDelegate(this.conf, glueMetastore, wh);

    snapshotActiveConf();
    if (!doesDefaultDBExist()) {
      createDefaultDatabase();
    }
  }

  /**
   * Currently used for unit tests
   */
  public static class Builder {

    private HiveConf conf;
    private Warehouse wh;
    private GlueClientFactory clientFactory;
    private AWSGlueMetastoreFactory metastoreFactory;
    private boolean createDefaults = true;
    private String catalogId;
    private GlueMetastoreClientDelegate glueMetastoreClientDelegate;

    public Builder withHiveConf(HiveConf conf) {
      this.conf = conf;
      return this;
    }

    public Builder withClientFactory(GlueClientFactory clientFactory) {
      this.clientFactory = clientFactory;
      return this;
    }

    public Builder withMetastoreFactory(AWSGlueMetastoreFactory metastoreFactory) {
      this.metastoreFactory = metastoreFactory;
      return this;
    }

    public Builder withCatalogId(String catalogId) {
      this.catalogId = catalogId;
      return this;
    }

    public Builder withGlueMetastoreClientDelegate(GlueMetastoreClientDelegate clientDelegate) {
      this.glueMetastoreClientDelegate = clientDelegate;
      return this;
    }

    public Builder withWarehouse(Warehouse wh) {
      this.wh = wh;
      return this;
    }

    public AWSCatalogMetastoreClient build() throws MetaException {
      return new AWSCatalogMetastoreClient(this);
    }

    public Builder createDefaults(boolean createDefaultDB) {
      this.createDefaults = createDefaultDB;
      return this;
    }
  }

  private AWSCatalogMetastoreClient(Builder builder) throws MetaException {
    conf = Objects.firstNonNull(builder.conf, new HiveConf());
    catalogToHiveConverter = new BaseCatalogToHiveConverter();
    
    if (builder.wh != null) {
      this.wh = builder.wh;
    } else {
      this.wh = new Warehouse(conf);
    }
    
    if (builder.catalogId != null) {
      this.catalogId = builder.catalogId;
    } else {
      this.catalogId = null;
    }

    GlueClientFactory clientFactory = Objects.firstNonNull(builder.clientFactory, new AWSGlueClientFactory(conf));
    AWSGlueMetastoreFactory metastoreFactory = Objects.firstNonNull(builder.metastoreFactory,
            new AWSGlueMetastoreFactory());
    glueClient = clientFactory.newClient();
    AWSGlueMetastore glueMetastore = metastoreFactory.newMetastore(conf);
    glueMetastoreClientDelegate = new GlueMetastoreClientDelegate(this.conf, glueMetastore, wh);

    if(builder.createDefaults && !doesDefaultDBExist()) {
      createDefaultDatabase();
    }
  }

  private boolean doesDefaultDBExist() throws MetaException {
    try {
      GetDatabaseRequest getDatabaseRequest = GetDatabaseRequest.builder().name("default").catalogId(catalogId).build();
      glueClient.getDatabase(getDatabaseRequest);
    } catch (EntityNotFoundException e) {
      return false;
    } catch (AwsServiceException e) {
      String msg = "Unable to verify existence of default database: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
    return true;
  }

  private void createDefaultDatabase() throws MetaException {
      org.apache.hadoop.hive.metastore.api.Database defaultDB = new org.apache.hadoop.hive.metastore.api.Database();
      defaultDB.setName("default");
      defaultDB.setDescription("Default Hive database");
      defaultDB.setLocationUri(wh.getDefaultDatabasePath("default").toString());

      org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet principalPrivilegeSet
            = new org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet();
      principalPrivilegeSet.setRolePrivileges(Maps.<String, List<org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo>>newHashMap());

      defaultDB.setPrivileges(principalPrivilegeSet);

      try {
          createDatabase(defaultDB);
      } catch (org.apache.hadoop.hive.metastore.api.AlreadyExistsException e) {
          logger.warn("database - default already exists. Ignoring..");
      } catch (Exception e) {
          logger.error("Unable to create default database", e);
      }
  }

  @Override
  public void createDatabase(Database database) throws InvalidObjectException,
        org.apache.hadoop.hive.metastore.api.AlreadyExistsException, MetaException, TException {
    glueMetastoreClientDelegate.createDatabase(database);
  }

  @Override
  public Database getDatabase(String name) throws NoSuchObjectException, MetaException, TException {
    return glueMetastoreClientDelegate.getDatabase(name);
  }

  @Override
  public List<String> getDatabases(String pattern) throws MetaException, TException {
    return glueMetastoreClientDelegate.getDatabases(pattern);
  }

  @Override
  public List<String> getAllDatabases() throws MetaException, TException {
      return getDatabases(".*");
  }

  @Override
  public void alterDatabase(String databaseName, Database database) throws NoSuchObjectException, MetaException,
        TException {
    glueMetastoreClientDelegate.alterDatabase(databaseName,database);
  }

  @Override
  public void dropDatabase(String name) throws NoSuchObjectException, InvalidOperationException, MetaException,
        TException {
      dropDatabase(name, true, false, false);
  }

  @Override
  public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb) throws NoSuchObjectException,
        InvalidOperationException, MetaException, TException {
      dropDatabase(name, deleteData, ignoreUnknownDb, false);
  }

  @Override
  public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade)
        throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    glueMetastoreClientDelegate.dropDatabase(name, deleteData, ignoreUnknownDb, cascade);
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition add_partition(org.apache.hadoop.hive.metastore.api.Partition partition)
        throws InvalidObjectException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException, MetaException,
        TException {
    glueMetastoreClientDelegate.addPartitions(Lists.newArrayList(partition), false, true);
    return partition;
  }

  @Override
  public int add_partitions(List<org.apache.hadoop.hive.metastore.api.Partition> partitions)
        throws InvalidObjectException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException, MetaException,
        TException {
    return glueMetastoreClientDelegate.addPartitions(partitions, false, true).size();
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> add_partitions(
      List<org.apache.hadoop.hive.metastore.api.Partition> partitions,
      boolean ifNotExists,
      boolean needResult
  ) throws TException {
    return glueMetastoreClientDelegate.addPartitions(partitions, ifNotExists, needResult);
  }

  @Override
  public int add_partitions_pspec(
      PartitionSpecProxy pSpec
  ) throws InvalidObjectException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException,
      MetaException, TException {
    return glueMetastoreClientDelegate.addPartitionsSpecProxy(pSpec);
  }

  @Override
  public void alterFunction(String dbName, String functionName, org.apache.hadoop.hive.metastore.api.Function newFunction) throws InvalidObjectException,
        MetaException, TException {
    glueMetastoreClientDelegate.alterFunction(dbName, functionName, newFunction);      
  }

  @Override
  public void alter_partition(String dbName, String tblName, org.apache.hadoop.hive.metastore.api.Partition partition) throws InvalidOperationException,
        MetaException, TException {
    glueMetastoreClientDelegate.alterPartitions(dbName, tblName, Lists.newArrayList(partition));
  }

  @Override
  public void alter_partition(String dbName, String tblName, org.apache.hadoop.hive.metastore.api.Partition partition, EnvironmentContext environmentContext)
        throws InvalidOperationException, MetaException, TException {
    glueMetastoreClientDelegate.alterPartitions(dbName, tblName, Lists.newArrayList(partition));
  }

  @Override
  public void alter_partitions(String dbName, String tblName, List<org.apache.hadoop.hive.metastore.api.Partition> partitions)
        throws InvalidOperationException, MetaException, TException {
    glueMetastoreClientDelegate.alterPartitions(dbName, tblName, partitions);
  }

  @Override
  public void alter_partitions(String dbName, String tblName, List<org.apache.hadoop.hive.metastore.api.Partition> partitions, EnvironmentContext environmentContext)
        throws InvalidOperationException, MetaException, TException {
    glueMetastoreClientDelegate.alterPartitions(dbName, tblName, partitions);
  }

  @Override
  public void alter_table(
      String dbName,
      String tblName,
      org.apache.hadoop.hive.metastore.api.Table table
  ) throws InvalidOperationException, MetaException, TException {
      alter_table(dbName, tblName, table, false);
  }

  @Override
  public void alter_table(
      String dbName,
      String tblName,
      org.apache.hadoop.hive.metastore.api.Table table,
      boolean cascade
  ) throws InvalidOperationException, MetaException, TException {
    EnvironmentContext environmentContext = null;
    if (cascade) {
      environmentContext = new EnvironmentContext();
      environmentContext.putToProperties("CASCADE", StatsSetupConst.TRUE);
    }
    glueMetastoreClientDelegate.alterTable(dbName, tblName, table, environmentContext);
  }

  @Override
  public void alter_table_with_environmentContext(
      String dbName,
      String tblName,
      org.apache.hadoop.hive.metastore.api.Table table,
      EnvironmentContext environmentContext
  ) throws InvalidOperationException, MetaException, TException {
    glueMetastoreClientDelegate.alterTable(dbName, tblName, table, environmentContext);
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition appendPartition(
      String dbName,
      String tblName,
      List<String> values
  ) throws InvalidObjectException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException, MetaException, TException {
    return glueMetastoreClientDelegate.appendPartition(dbName, tblName, values);
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition appendPartition(String dbName, String tblName, String partitionName) throws InvalidObjectException,
      org.apache.hadoop.hive.metastore.api.AlreadyExistsException, MetaException, TException {
    List<String> partVals = partitionNameToVals(partitionName);
    return glueMetastoreClientDelegate.appendPartition(dbName, tblName, partVals);
  }

  @Override
  public boolean create_role(org.apache.hadoop.hive.metastore.api.Role role) throws MetaException, TException {
    return glueMetastoreClientDelegate.createRole(role);
  }

  @Override
  public boolean drop_role(String roleName) throws MetaException, TException {
    return glueMetastoreClientDelegate.dropRole(roleName);
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Role> list_roles(
      String principalName, org.apache.hadoop.hive.metastore.api.PrincipalType principalType
  ) throws MetaException, TException {
    return glueMetastoreClientDelegate.listRoles(principalName, principalType);
  }

  @Override
  public List<String> listRoleNames() throws MetaException, TException {
    return glueMetastoreClientDelegate.listRoleNames();
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse get_principals_in_role(
      org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest request) throws MetaException, TException {
    return glueMetastoreClientDelegate.getPrincipalsInRole(request);
  }

  @Override
  public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(
      GetRoleGrantsForPrincipalRequest request) throws MetaException, TException {
    return glueMetastoreClientDelegate.getRoleGrantsForPrincipal(request);
  }

  @Override
  public boolean grant_role(
      String roleName,
      String userName,
      org.apache.hadoop.hive.metastore.api.PrincipalType principalType,
      String grantor, org.apache.hadoop.hive.metastore.api.PrincipalType grantorType,
      boolean grantOption
  ) throws MetaException, TException {
    return glueMetastoreClientDelegate.grantRole(roleName, userName, principalType, grantor, grantorType, grantOption);
  }

  @Override
  public boolean revoke_role(
      String roleName,
      String userName,
      org.apache.hadoop.hive.metastore.api.PrincipalType principalType,
      boolean grantOption
  ) throws MetaException, TException {
    return glueMetastoreClientDelegate.revokeRole(roleName, userName, principalType, grantOption);
  }

  @Override
  public void cancelDelegationToken(String tokenStrForm) throws MetaException, TException {
    glueMetastoreClientDelegate.cancelDelegationToken(tokenStrForm);
  }

  @Override
  public String getTokenStrForm() throws IOException {
    return glueMetastoreClientDelegate.getTokenStrForm();
  }

  @Override
  public boolean addToken(String tokenIdentifier, String delegationToken) throws TException {
    return glueMetastoreClientDelegate.addToken(tokenIdentifier, delegationToken);
  }

  @Override
  public boolean removeToken(String tokenIdentifier) throws TException {
    return glueMetastoreClientDelegate.removeToken(tokenIdentifier);
  }

  @Override
  public String getToken(String tokenIdentifier) throws TException {
    return glueMetastoreClientDelegate.getToken(tokenIdentifier);
  }

  @Override
  public List<String> getAllTokenIdentifiers() throws TException {
    return glueMetastoreClientDelegate.getAllTokenIdentifiers();
  }

  @Override
  public int addMasterKey(String key) throws MetaException, TException {
    return glueMetastoreClientDelegate.addMasterKey(key);
  }

  @Override
  public void updateMasterKey(Integer seqNo, String key) throws NoSuchObjectException, MetaException, TException {
    glueMetastoreClientDelegate.updateMasterKey(seqNo, key);
  }

  @Override
  public boolean removeMasterKey(Integer keySeq) throws TException {
    return glueMetastoreClientDelegate.removeMasterKey(keySeq);
  }

  @Override
  public String[] getMasterKeys() throws TException {
    return glueMetastoreClientDelegate.getMasterKeys();
  }

  @Override
  public LockResponse checkLock(long lockId)
      throws NoSuchTxnException, TxnAbortedException, NoSuchLockException, TException {
    return glueMetastoreClientDelegate.checkLock(lockId);
  }

  @Override
  public void close() {
      currentMetaVars = null;
  }

  @Override
  public void commitTxn(long txnId) throws NoSuchTxnException, TxnAbortedException, TException {
    glueMetastoreClientDelegate.commitTxn(txnId);
  }

  @Override
  public void abortTxns(List<Long> txnIds) throws TException {
    glueMetastoreClientDelegate.abortTxns(txnIds);
  }

  @Deprecated
  public void compact(
      String dbName,
      String tblName,
      String partitionName,
      CompactionType compactionType
  ) throws TException {
    glueMetastoreClientDelegate.compact(dbName, tblName, partitionName, compactionType);
  }

  @Deprecated
  public void compact(
      String dbName,
      String tblName,
      String partitionName,
      CompactionType compactionType,
      Map<String, String> tblProperties
  ) throws TException {
    glueMetastoreClientDelegate.compact(dbName, tblName, partitionName, compactionType, tblProperties);
  }

  @Override
  public CompactionResponse compact2(
      String dbName,
      String tblName,
      String partitionName,
      CompactionType compactionType,
      Map<String, String> tblProperties
  ) throws TException {
    return glueMetastoreClientDelegate.compact2(dbName, tblName, partitionName, compactionType, tblProperties);
  }

  @Override
  public void createFunction(org.apache.hadoop.hive.metastore.api.Function function) throws InvalidObjectException, MetaException, TException {
    glueMetastoreClientDelegate.createFunction(function);
  }

  @Override
  public void createTable(org.apache.hadoop.hive.metastore.api.Table tbl) throws org.apache.hadoop.hive.metastore.api.AlreadyExistsException, InvalidObjectException, MetaException,
        NoSuchObjectException, TException {
    glueMetastoreClientDelegate.createTable(tbl);
  }

  @Override
  public boolean deletePartitionColumnStatistics(
      String dbName, String tableName, String partName, String colName
  ) throws NoSuchObjectException, MetaException, InvalidObjectException,
      TException, org.apache.hadoop.hive.metastore.api.InvalidInputException {
    return glueMetastoreClientDelegate.deletePartitionColumnStatistics(dbName, tableName, partName, colName);
  }

  @Override
  public boolean deleteTableColumnStatistics(
      String dbName, String tableName, String colName
  ) throws NoSuchObjectException, MetaException, InvalidObjectException,
      TException, org.apache.hadoop.hive.metastore.api.InvalidInputException {
    return glueMetastoreClientDelegate.deleteTableColumnStatistics(dbName, tableName, colName);
  }

  @Override
  public void dropFunction(String dbName, String functionName) throws MetaException, NoSuchObjectException,
        InvalidObjectException, org.apache.hadoop.hive.metastore.api.InvalidInputException, TException {
    glueMetastoreClientDelegate.dropFunction(dbName, functionName);
  }

  private void deleteParentRecursive(Path parent, int depth, boolean mustPurge) throws IOException, MetaException {
      if (depth > 0 && parent != null && wh.isWritable(parent) && wh.isEmpty(parent)) {
          wh.deleteDir(parent, true, mustPurge, true);
          deleteParentRecursive(parent.getParent(), depth - 1, mustPurge);
      }
  }

  // This logic is taken from HiveMetaStore#isMustPurge
  private boolean isMustPurge(org.apache.hadoop.hive.metastore.api.Table table, boolean ifPurge) {
      return (ifPurge || "true".equalsIgnoreCase(table.getParameters().get("auto.purge")));
  }

  @Override
  public boolean dropPartition(String dbName, String tblName, List<String> values, boolean deleteData) throws
        NoSuchObjectException, MetaException, TException {
    return glueMetastoreClientDelegate.dropPartition(dbName, tblName, values, false, deleteData, false);
  }

  @Override
  public boolean dropPartition(String dbName, String tblName, List<String> values, PartitionDropOptions options) throws TException {
    return glueMetastoreClientDelegate.dropPartition(dbName, tblName, values, options.ifExists, options.deleteData, options.purgeData);
  }

  @Override
  public boolean dropPartition(String dbName, String tblName, String partitionName, boolean deleteData)
      throws NoSuchObjectException, MetaException, TException {
    List<String> values = partitionNameToVals(partitionName);
    return glueMetastoreClientDelegate.dropPartition(dbName, tblName, values, false, deleteData, false);
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> dropPartitions(
        String dbName, String tblName, List<ObjectPair<Integer, byte[]>> partExprs,
        boolean deleteData, boolean ifExists)
        throws NoSuchObjectException, MetaException, TException {
      return dropPartitions_core(dbName, tblName, partExprs, deleteData, false);
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> dropPartitions(
        String dbName,
        String tblName, List<ObjectPair<Integer, byte[]>> partExprs,
        boolean deleteData,
        boolean ifExists,
        boolean needResults)
        throws NoSuchObjectException, MetaException, TException {
      return dropPartitions_core(dbName, tblName, partExprs, deleteData, false);
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> dropPartitions(
        String dbName,
        String tblName,
        List<ObjectPair<Integer, byte[]>> partExprs,
        PartitionDropOptions options)
        throws TException {
      return dropPartitions_core(dbName, tblName, partExprs, options.deleteData, options.purgeData);
  }

  private List<org.apache.hadoop.hive.metastore.api.Partition> dropPartitions_core(
      String databaseName,
      String tableName,
      List<ObjectPair<Integer, byte[]>> partExprs,
      boolean deleteData,
      boolean purgeData
  ) throws TException {
    List<org.apache.hadoop.hive.metastore.api.Partition> deleted = Lists.newArrayList();
    for (ObjectPair<Integer, byte[]> expr : partExprs) {
      byte[] tmp = expr.getSecond();
      String exprString = ExpressionHelper.convertHiveExpressionToCatalogExpression(tmp);
      List<Partition> catalogPartitionsToDelete = glueMetastoreClientDelegate.getCatalogPartitions(databaseName, tableName, exprString, -1);
      deleted.addAll(batchDeletePartitions(databaseName, tableName, catalogPartitionsToDelete, deleteData, purgeData));
    }
    return deleted;
  }

  private List<org.apache.hadoop.hive.metastore.api.Partition> batchDeletePartitions(
        final String dbName, final String tableName, final List<Partition> partitionsToDelete,
        final boolean deleteData, final boolean purgeData) throws TException {

      List<org.apache.hadoop.hive.metastore.api.Partition> deleted = Lists.newArrayList();
      if (partitionsToDelete == null) {
          return deleted;
      }

      validateBatchDeletePartitionsArguments(dbName, tableName, partitionsToDelete);

      List<Future<BatchDeletePartitionsHelper>> batchDeletePartitionsFutures = Lists.newArrayList();

      int numOfPartitionsToDelete = partitionsToDelete.size();
      for (int i = 0; i < numOfPartitionsToDelete; i += BATCH_DELETE_PARTITIONS_PAGE_SIZE) {
          int j = Math.min(i + BATCH_DELETE_PARTITIONS_PAGE_SIZE, numOfPartitionsToDelete);
          final List<Partition> partitionsOnePage = partitionsToDelete.subList(i, j);

          batchDeletePartitionsFutures.add(BATCH_DELETE_PARTITIONS_THREAD_POOL.submit(new Callable<BatchDeletePartitionsHelper>() {
              @Override
              public BatchDeletePartitionsHelper call() throws Exception {
                  return new BatchDeletePartitionsHelper(glueClient, dbName, tableName, catalogId, partitionsOnePage).deletePartitions();
              }
          }));
      }

      TException tException = null;
      for (Future<BatchDeletePartitionsHelper> future : batchDeletePartitionsFutures) {
          try {
              BatchDeletePartitionsHelper batchDeletePartitionsHelper = future.get();
              for (Partition partition : batchDeletePartitionsHelper.getPartitionsDeleted()) {
                  org.apache.hadoop.hive.metastore.api.Partition hivePartition =
                      catalogToHiveConverter.convertPartition(partition);
                  try {
                      performDropPartitionPostProcessing(dbName, tableName, hivePartition, deleteData, purgeData);
                  } catch (TException e) {
                      logger.error("Drop partition directory failed.", e);
                      tException = tException == null ? e : tException;
                  }
                  deleted.add(hivePartition);
              }
              tException = tException == null ? batchDeletePartitionsHelper.getFirstTException() : tException;
          } catch (Exception e) {
              logger.error("Exception thrown by BatchDeletePartitions thread pool. ", e);
          }
      }

      if (tException != null) {
          throw tException;
      }
      return deleted;
  }

  private void validateBatchDeletePartitionsArguments(final String dbName, final String tableName,
                                                      final List<Partition> partitionsToDelete) {

      Preconditions.checkArgument(dbName != null, "Database name cannot be null");
      Preconditions.checkArgument(tableName != null, "Table name cannot be null");
      for (Partition partition : partitionsToDelete) {
          Preconditions.checkArgument(dbName.equals(partition.databaseName()), "Database name cannot be null");
          Preconditions.checkArgument(tableName.equals(partition.tableName()), "Table name cannot be null");
          Preconditions.checkArgument(partition.values() != null, "Partition values cannot be null");
      }
  }

  // Preserve the logic from Hive metastore
  private void performDropPartitionPostProcessing(String dbName, String tblName,
                                                  org.apache.hadoop.hive.metastore.api.Partition partition, boolean deleteData, boolean ifPurge)
        throws MetaException, NoSuchObjectException, TException {
      if (deleteData && partition.getSd() != null && partition.getSd().getLocation() != null) {
          Path partPath = new Path(partition.getSd().getLocation());
          org.apache.hadoop.hive.metastore.api.Table table = getTable(dbName, tblName);
          if (isExternalTable(table)){
              //Don't delete external table data
              return;
          }
          boolean mustPurge = isMustPurge(table, ifPurge);
          wh.deleteDir(partPath, true, mustPurge, true);
          try {
              List<String> values = partition.getValues();
              deleteParentRecursive(partPath.getParent(), values.size() - 1, mustPurge);
          } catch (IOException e) {
              throw new MetaException(e.getMessage());
          }
      }
  }

  @Override
  public void dropTable(String dbname, String tableName) throws MetaException, TException, NoSuchObjectException {
      dropTable(dbname, tableName, true, true, false);
  }

  @Override
  public void dropTable(String dbname, String tableName, boolean deleteData, boolean ignoreUnknownTab)
        throws MetaException, TException, NoSuchObjectException {
      dropTable(dbname, tableName, deleteData, ignoreUnknownTab, false);
  }

  @Override
  public void dropTable(String dbname, String tableName, boolean deleteData, boolean ignoreUnknownTab, boolean ifPurge)
        throws MetaException, TException, NoSuchObjectException {
    glueMetastoreClientDelegate.dropTable(dbname, tableName, deleteData, ignoreUnknownTab, ifPurge);
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition exchange_partition(
      Map<String, String> partitionSpecs,
      String srcDb,
      String srcTbl,
      String dstDb,
      String dstTbl
  ) throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
    return glueMetastoreClientDelegate.exchangePartition(partitionSpecs, srcDb, srcTbl, dstDb, dstTbl);
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> exchange_partitions(
      Map<String, String> partitionSpecs,
      String sourceDb,
      String sourceTbl,
      String destDb,
      String destTbl
  ) throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
    return glueMetastoreClientDelegate.exchangePartitions(partitionSpecs, sourceDb, sourceTbl, destDb, destTbl);
  }

  @Override
  public AggrStats getAggrColStatsFor(String dbName, String tblName, List<String> colNames, List<String> partName)
      throws NoSuchObjectException, MetaException, TException {
    return glueMetastoreClientDelegate.getAggrColStatsFor(dbName, tblName, colNames, partName);
  }

  @Override
  public List<String> getAllTables(String dbname) throws MetaException, TException, UnknownDBException {
      return getTables(dbname, ".*");
  }

  @Override
  public String getConfigValue(String name, String defaultValue) throws TException, ConfigValSecurityException {
      if(!Pattern.matches("(hive|hdfs|mapred).*", name)) {
          throw new ConfigValSecurityException("For security reasons, the config key " + name + " cannot be accessed");
      }

      return conf.get(name, defaultValue);
  }

  @Override
  public String getDelegationToken(String owner, String renewerKerberosPrincipalName) throws MetaException, TException {
    return glueMetastoreClientDelegate.getDelegationToken(owner, renewerKerberosPrincipalName);
  }

  @Override
  public List<FieldSchema> getFields(String db, String tableName) throws MetaException, TException,
        UnknownTableException, UnknownDBException {
      return glueMetastoreClientDelegate.getFields(db, tableName);
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Function getFunction(String dbName, String functionName) throws MetaException, TException {
    try {
      return glueMetastoreClientDelegate.getFunction(dbName, functionName);
    } catch (NoSuchObjectException e) {
      throw new NoSuchObjectException(functionName + " does not exist");
    } catch (AwsServiceException e) {
      logger.error(e);
      throw catalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to get Function: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  @Override
  public List<String> getFunctions(String dbName, String pattern) throws MetaException, TException {
    if (pattern.equals("*")) {
      pattern = ".*";
    }

    return glueMetastoreClientDelegate.getFunctions(dbName, pattern);
  }

  @Override
  public GetAllFunctionsResponse getAllFunctions() throws MetaException, TException {
    return glueMetastoreClientDelegate.getAllFunctions();
  }

  @Override
  public String getMetaConf(String key) throws MetaException, TException {
      ConfVars metaConfVar = HiveConf.getMetaConf(key);
      if (metaConfVar == null) {
          throw new MetaException("Invalid configuration key " + key);
      }
      return conf.get(key, metaConfVar.getDefaultValue());
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition getPartition(String dbName, String tblName, List<String> values)
      throws NoSuchObjectException, MetaException, TException {
    return glueMetastoreClientDelegate.getPartition(dbName, tblName, values);
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition getPartition(String dbName, String tblName, String partitionName)
      throws MetaException, UnknownTableException, NoSuchObjectException, TException {
    return glueMetastoreClientDelegate.getPartition(dbName, tblName, partitionName);
  }

  @Override
  public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
      String dbName,
      String tableName,
      List<String> partitionNames,
      List<String> columnNames
  ) throws NoSuchObjectException, MetaException, TException {
    return glueMetastoreClientDelegate.getPartitionColumnStatistics(dbName, tableName, partitionNames, columnNames);
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition getPartitionWithAuthInfo(
        String databaseName, String tableName, List<String> values,
        String userName, List<String> groupNames)
        throws MetaException, UnknownTableException, NoSuchObjectException, TException {

      org.apache.hadoop.hive.metastore.api.Partition partition = getPartition(databaseName, tableName, values);
      org.apache.hadoop.hive.metastore.api.Table table = getTable(databaseName, tableName);
      if ("TRUE".equalsIgnoreCase(table.getParameters().get("PARTITION_LEVEL_PRIVILEGE"))) {
          String partName = Warehouse.makePartName(table.getPartitionKeys(), values);
          HiveObjectRef obj = new HiveObjectRef();
          obj.setObjectType(HiveObjectType.PARTITION);
          obj.setDbName(databaseName);
          obj.setObjectName(tableName);
          obj.setPartValues(values);
          org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet privilegeSet =
                this.get_privilege_set(obj, userName, groupNames);
          partition.setPrivileges(privilegeSet);
      }

      return partition;
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> getPartitionsByNames(
        String databaseName, String tableName, List<String> partitionNames)
        throws NoSuchObjectException, MetaException, TException {
    return glueMetastoreClientDelegate.getPartitionsByNames(databaseName, tableName, partitionNames);
  }

  @Override
  public List<FieldSchema> getSchema(String db, String tableName) throws MetaException, TException, UnknownTableException,
        UnknownDBException {
    return glueMetastoreClientDelegate.getSchema(db, tableName);    
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Table getTable(String dbName, String tableName)
        throws MetaException, TException, NoSuchObjectException {
    return glueMetastoreClientDelegate.getTable(dbName, tableName);
  }

  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName, List<String> colNames)
      throws NoSuchObjectException, MetaException, TException {
    return glueMetastoreClientDelegate.getTableColumnStatistics(dbName, tableName, colNames);
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Table> getTableObjectsByName(String dbName, List<String> tableNames) throws MetaException,
        InvalidOperationException, UnknownDBException, TException {
      List<org.apache.hadoop.hive.metastore.api.Table> hiveTables = Lists.newArrayList();
      for(String tableName : tableNames) {
          hiveTables.add(getTable(dbName, tableName));
      }

      return hiveTables;
  }

  @Override
  public List<String> getTables(String dbname, String tablePattern) throws MetaException, TException, UnknownDBException {
    return glueMetastoreClientDelegate.getTables(dbname, tablePattern);
  }

  @Override
  public List<String> getTables(String dbname, String tablePattern, TableType tableType)
      throws MetaException, TException, UnknownDBException {
    return glueMetastoreClientDelegate.getTables(dbname, tablePattern, tableType);
  }

  @Override
  public List<TableMeta> getTableMeta(String dbPatterns, String tablePatterns, List<String> tableTypes)
      throws MetaException, TException, UnknownDBException {
    return glueMetastoreClientDelegate.getTableMeta(dbPatterns, tablePatterns, tableTypes);
  }

  @Override
  public ValidTxnList getValidTxns() throws TException {
    return glueMetastoreClientDelegate.getValidTxns();
  }

  @Override
  public ValidTxnList getValidTxns(long currentTxn) throws TException {
    return glueMetastoreClientDelegate.getValidTxns(currentTxn);
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet get_privilege_set(
      HiveObjectRef obj,
      String user, List<String> groups
  ) throws MetaException, TException {
    return glueMetastoreClientDelegate.getPrivilegeSet(obj, user, groups);
  }

  @Override
  public boolean grant_privileges(org.apache.hadoop.hive.metastore.api.PrivilegeBag privileges)
      throws MetaException, TException {
    return glueMetastoreClientDelegate.grantPrivileges(privileges);
  }

  @Override
  public boolean revoke_privileges(
      org.apache.hadoop.hive.metastore.api.PrivilegeBag privileges,
      boolean grantOption
  ) throws MetaException, TException {
    return glueMetastoreClientDelegate.revokePrivileges(privileges, grantOption);
  }

  @Override
  public void heartbeat(long txnId, long lockId)
      throws NoSuchLockException, NoSuchTxnException, TxnAbortedException, TException {
    glueMetastoreClientDelegate.heartbeat(txnId, lockId);
  }

  @Override
  public HeartbeatTxnRangeResponse heartbeatTxnRange(long min, long max) throws TException {
    return glueMetastoreClientDelegate.heartbeatTxnRange(min, max);
  }

  @Override
  public boolean isCompatibleWith(Configuration conf) {
    Map<String, String> currentMetaVarsCopy = currentMetaVars;
    if (currentMetaVarsCopy == null) {
          return false; // recreate
      }
      boolean compatible = true;
      for (ConfVars oneVar : HiveConf.metaVars) {
          String oldVar = currentMetaVarsCopy.get(oneVar.varname);
          String newVar = conf.get(oneVar.varname, "");
          if (oldVar == null ||
                (oneVar.isCaseSensitive() ? !oldVar.equals(newVar) : !oldVar.equalsIgnoreCase(newVar))) {
              logger.info("Mestastore configuration " + oneVar.varname +
                    " changed from " + oldVar + " to " + newVar);
              compatible = false;
          }
      }
      return compatible;
  }

  @Override
  public void setHiveAddedJars(String s) {
      throw new UnsupportedOperationException("setHiveAddedJars is not supported");
  }

  @Override
  public boolean isLocalMetaStore() {
    return false;
  }

  private void snapshotActiveConf() {
      currentMetaVars = new HashMap<String, String>(HiveConf.metaVars.length);
      for (ConfVars oneVar : HiveConf.metaVars) {
          currentMetaVars.put(oneVar.varname, conf.get(oneVar.varname, ""));
      }
  }

  @Override
  public boolean isPartitionMarkedForEvent(String dbName, String tblName, Map<String, String> partKVs, PartitionEventType eventType)
      throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
      UnknownPartitionException, InvalidPartitionException {
    return glueMetastoreClientDelegate.isPartitionMarkedForEvent(dbName, tblName, partKVs, eventType);
  }

  @Override
  public List<String> listPartitionNames(String dbName, String tblName, short max)
        throws MetaException, TException {
      try {
          return listPartitionNames(dbName, tblName, (List<String>) null, max);
      } catch (NoSuchObjectException e) {
          return Collections.emptyList();
      }
  }

  @Override
  public List<String> listPartitionNames(String databaseName, String tableName,
                                         List<String> values, short max)
        throws MetaException, TException, NoSuchObjectException {
    return glueMetastoreClientDelegate.listPartitionNames(databaseName, tableName, values, max);
  }

  @Override
  public PartitionValuesResponse listPartitionValues(PartitionValuesRequest partitionValuesRequest) throws TException {
    return glueMetastoreClientDelegate.listPartitionValues(partitionValuesRequest);
  }

  @Override
  public int getNumPartitionsByFilter(String dbName, String tableName, String filter)
      throws MetaException, NoSuchObjectException, TException {
    return glueMetastoreClientDelegate.getNumPartitionsByFilter(dbName, tableName, filter);
  }

  @Override
  public PartitionSpecProxy listPartitionSpecs(String dbName, String tblName, int max) throws TException {
    return glueMetastoreClientDelegate.listPartitionSpecs(dbName, tblName, max);
  }

  @Override
  public PartitionSpecProxy listPartitionSpecsByFilter(String dbName, String tblName, String filter, int max)
      throws MetaException, NoSuchObjectException, TException {
    return glueMetastoreClientDelegate.listPartitionSpecsByFilter(dbName, tblName, filter, max);
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitions(
        String dbName, String tblName, short max)
        throws NoSuchObjectException, MetaException, TException {
      return listPartitions(dbName, tblName, (List<String>) null, max);
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitions(
      String databaseName,
      String tableName,
      List<String> values,
      short max
  ) throws NoSuchObjectException, MetaException, TException {
    String expression = null;
    if (values != null) {
        org.apache.hadoop.hive.metastore.api.Table table = getTable(databaseName, tableName);
        expression = ExpressionHelper.buildExpressionFromPartialSpecification(table, values);
    }
    return glueMetastoreClientDelegate.getPartitions(databaseName, tableName, expression, (long) max);
  }

  @Override
  public boolean listPartitionsByExpr(
      String databaseName,
      String tableName,
      byte[] expr,
      String defaultPartitionName,
      short max,
      List<org.apache.hadoop.hive.metastore.api.Partition> result
  ) throws TException {
    checkNotNull(result, "The result argument cannot be null.");

    String catalogExpression =  ExpressionHelper.convertHiveExpressionToCatalogExpression(expr);
    List<org.apache.hadoop.hive.metastore.api.Partition> partitions =
        glueMetastoreClientDelegate.getPartitions(databaseName, tableName, catalogExpression, (long) max);
    result.addAll(partitions);

    return false;
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsByFilter(
      String databaseName,
      String tableName,
      String filter,
      short max
  ) throws MetaException, NoSuchObjectException, TException {
    if (StringUtils.isNotBlank(filter)) {
        filter = ExpressionHelper.replaceDoubleQuoteWithSingleQuotes(filter);
    }
    return glueMetastoreClientDelegate.getPartitions(databaseName, tableName, filter, (long) max);
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsWithAuthInfo(String database, String table, short maxParts,
                                                                                         String user, List<String> groups)
        throws MetaException, TException, NoSuchObjectException {
      List<org.apache.hadoop.hive.metastore.api.Partition> partitions = listPartitions(database, table, maxParts);

      for (org.apache.hadoop.hive.metastore.api.Partition p : partitions) {
          HiveObjectRef obj = new HiveObjectRef();
          obj.setObjectType(HiveObjectType.PARTITION);
          obj.setDbName(database);
          obj.setObjectName(table);
          obj.setPartValues(p.getValues());
          org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet set = this.get_privilege_set(obj, user, groups);
          p.setPrivileges(set);
      }

      return partitions;
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsWithAuthInfo(String database, String table,
                                                                                         List<String> partVals, short maxParts,
                                                                                         String user, List<String> groups) throws MetaException, TException, NoSuchObjectException {
    List<org.apache.hadoop.hive.metastore.api.Partition> partitions = listPartitions(database, table, partVals, maxParts);

    for (org.apache.hadoop.hive.metastore.api.Partition p : partitions) {
      HiveObjectRef obj = new HiveObjectRef();
      obj.setObjectType(HiveObjectType.PARTITION);
      obj.setDbName(database);
      obj.setObjectName(table);
      obj.setPartValues(p.getValues());
      org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet set;
      try {
        set = get_privilege_set(obj, user, groups);
      } catch (MetaException e) {
        logger.info(String.format("No privileges found for user: %s, "
            + "groups: [%s]", user, LoggingHelper.concatCollectionToStringForLogging(groups, ",")));
        set = new org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet();
      }
      p.setPrivileges(set);
    }

    return partitions;
  }

  @Override
  public List<String> listTableNamesByFilter(String dbName, String filter, short maxTables) throws
        TException, InvalidOperationException, UnknownDBException {
    return glueMetastoreClientDelegate.listTableNamesByFilter(dbName, filter, maxTables);
  }

  @Override
  public List<HiveObjectPrivilege> list_privileges(
      String principal,
      org.apache.hadoop.hive.metastore.api.PrincipalType principalType,
      HiveObjectRef objectRef
  ) throws MetaException, TException {
    return glueMetastoreClientDelegate.listPrivileges(principal, principalType, objectRef);
  }

  @Override
  public LockResponse lock(LockRequest lockRequest) throws NoSuchTxnException, TxnAbortedException, TException {
    return glueMetastoreClientDelegate.lock(lockRequest);
  }

  @Override
  public void markPartitionForEvent(
      String dbName,
      String tblName,
      Map<String, String> partKVs,
      PartitionEventType eventType
  ) throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
      UnknownPartitionException, InvalidPartitionException {
    glueMetastoreClientDelegate.markPartitionForEvent(dbName, tblName, partKVs, eventType);
  }

  @Override
  public long openTxn(String user) throws TException {
    return glueMetastoreClientDelegate.openTxn(user);
  }

  @Override
  public OpenTxnsResponse openTxns(String user, int numTxns) throws TException {
    return glueMetastoreClientDelegate.openTxns(user, numTxns);
  }

  @Override
  public Map<String, String> partitionNameToSpec(String name) throws MetaException, TException {
      if (name.length() == 0) {
          return new HashMap<String, String>();
      }
      return Warehouse.makeSpecFromName(name);
  }

  @Override
  public List<String> partitionNameToVals(String name) throws MetaException, TException {
    return glueMetastoreClientDelegate.partitionNameToVals(name);
  }

  @Override
  public void reconnect() throws MetaException {
      logger.debug("reconnect() was called.");
  }

  @Override
  public void renamePartition(String dbName, String tblName, List<String> partitionValues,
                              org.apache.hadoop.hive.metastore.api.Partition newPartition)
        throws InvalidOperationException, MetaException, TException {

      setDDLTime(newPartition);
      org.apache.hadoop.hive.metastore.api.Table tbl;
      org.apache.hadoop.hive.metastore.api.Partition oldPart;

      try {
          tbl = getTable(dbName, tblName);
          oldPart = getPartition(dbName, tblName, partitionValues);
      } catch(NoSuchObjectException e) {
          throw new InvalidOperationException(e.getMessage());
      }

      if(newPartition.getSd() == null || oldPart.getSd() == null ) {
          throw new InvalidOperationException("Storage descriptor cannot be null");
      }

      if (!Strings.isNullOrEmpty(tbl.getTableType()) && tbl.getTableType().equals(TableType.EXTERNAL_TABLE.toString())) {
          newPartition.getSd().setLocation(oldPart.getSd().getLocation());
          renamePartitionInCatalog(dbName, tblName, partitionValues, newPartition);
      } else {

          Path destPath = getDestinationPathForRename(dbName, tbl, newPartition);
          Path srcPath = new Path(oldPart.getSd().getLocation());
          FileSystem srcFs = wh.getFs(srcPath);
          FileSystem destFs = wh.getFs(destPath);

          verifyDestinationLocation(srcFs, destFs, srcPath, destPath, tbl, newPartition);
          newPartition.getSd().setLocation(destPath.toString());

          renamePartitionInCatalog(dbName, tblName, partitionValues, newPartition);
          boolean success = true;
          try{
              if (srcFs.exists(srcPath)) {
                  Path destParentPath = destPath.getParent();
                  if (!hiveShims.mkdirs(wh, destParentPath)) {
                      throw new IOException("Unable to create path " + destParentPath);
                  }
                  wh.renameDir(srcPath, destPath, true);
              }
          } catch (IOException e) {
              success = false;
              throw new InvalidOperationException("Unable to access old location "
                    + srcPath + " for partition " + tbl.getDbName() + "."
                    + tbl.getTableName() + " " + partitionValues);
          } finally {
              if(!success) {
                  renamePartitionInCatalog(dbName, tblName, newPartition.getValues(), oldPart);
              }
          }
      }
  }

  private void verifyDestinationLocation(FileSystem srcFs, FileSystem destFs, Path srcPath, Path destPath, org.apache.hadoop.hive.metastore.api.Table tbl, org.apache.hadoop.hive.metastore.api.Partition newPartition)
        throws InvalidOperationException {
      String oldPartLoc = srcPath.toString();
      String newPartLoc = destPath.toString();

      if (!FileUtils.equalsFileSystem(srcFs, destFs)) {
          throw new InvalidOperationException("table new location " + destPath
                + " is on a different file system than the old location "
                + srcPath + ". This operation is not supported");
      }
      try {
          srcFs.exists(srcPath);
          if (newPartLoc.compareTo(oldPartLoc) != 0 && destFs.exists(destPath)) {
              throw new InvalidOperationException("New location for this partition "
                    + tbl.getDbName() + "." + tbl.getTableName() + "." + newPartition.getValues()
                    + " already exists : " + destPath);
          }
      } catch (IOException e) {
          throw new InvalidOperationException("Unable to access new location "
                + destPath + " for partition " + tbl.getDbName() + "."
                + tbl.getTableName() + " " + newPartition.getValues());
      }
  }

  private Path getDestinationPathForRename(String dbName, org.apache.hadoop.hive.metastore.api.Table tbl, org.apache.hadoop.hive.metastore.api.Partition newPartition)
        throws InvalidOperationException, MetaException, TException {
      try {
          Path destPath = new Path(hiveShims.getDefaultTablePath(getDatabase(dbName), tbl.getTableName(), wh),
                Warehouse.makePartName(tbl.getPartitionKeys(), newPartition.getValues()));
          return constructRenamedPath(destPath, new Path(newPartition.getSd().getLocation()));
      } catch (NoSuchObjectException e) {
          throw new InvalidOperationException(
                "Unable to change partition or table. Database " + dbName + " does not exist"
                      + " Check metastore logs for detailed stack." + e.getMessage());
      }
  }

  private void setDDLTime(org.apache.hadoop.hive.metastore.api.Partition partition) {
      if (partition.getParameters() == null ||
            partition.getParameters().get(hive_metastoreConstants.DDL_TIME) == null ||
            Integer.parseInt(partition.getParameters().get(hive_metastoreConstants.DDL_TIME)) == 0) {
          partition.putToParameters(hive_metastoreConstants.DDL_TIME, Long.toString(System
                .currentTimeMillis() / 1000));
      }
  }

  private void renamePartitionInCatalog(String databaseName, String tableName,
                                        List<String> partitionValues, org.apache.hadoop.hive.metastore.api.Partition newPartition)
        throws InvalidOperationException, MetaException, TException {
    glueMetastoreClientDelegate.renamePartitionInCatalog(databaseName, tableName, partitionValues, newPartition);
  }

  @Override
  public long renewDelegationToken(String tokenStrForm) throws MetaException, TException {
    return glueMetastoreClientDelegate.renewDelegationToken(tokenStrForm);
  }

  @Override
  public void rollbackTxn(long txnId) throws NoSuchTxnException, TException {
    glueMetastoreClientDelegate.rollbackTxn(txnId);
  }

  @Override
  public void setMetaConf(String key, String value) throws MetaException, TException {
      ConfVars confVar = HiveConf.getMetaConf(key);
      if (confVar == null) {
          throw new MetaException("Invalid configuration key " + key);
      }
      String validate = confVar.validate(value);
      if (validate != null) {
          throw new MetaException("Invalid configuration value " + value + " for key " + key + " by " + validate);
      }
      conf.set(key, value);
  }

  @Override
  public boolean setPartitionColumnStatistics(org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest request)
      throws NoSuchObjectException, InvalidObjectException,
      MetaException, TException, org.apache.hadoop.hive.metastore.api.InvalidInputException {
    return glueMetastoreClientDelegate.setPartitionColumnStatistics(request);
  }

  @Override
  public void flushCache() {
    //no op
  }

  @Override
  public Iterable<Map.Entry<Long, ByteBuffer>> getFileMetadata(List<Long> fileIds) throws TException {
    return glueMetastoreClientDelegate.getFileMetadata(fileIds);
  }

  @Override
  public Iterable<Map.Entry<Long, MetadataPpdResult>> getFileMetadataBySarg(
      List<Long> fileIds,
      ByteBuffer sarg,
      boolean doGetFooters
  ) throws TException {
    return glueMetastoreClientDelegate.getFileMetadataBySarg(fileIds, sarg, doGetFooters);
  }

  @Override
  public void clearFileMetadata(List<Long> fileIds) throws TException {
    glueMetastoreClientDelegate.clearFileMetadata(fileIds);
  }

  @Override
  public void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata) throws TException {
    glueMetastoreClientDelegate.putFileMetadata(fileIds, metadata);
  }

  @Override
  public boolean isSameConfObj(Configuration conf) {
    return this.conf == conf;
  }

  @Override
  public boolean cacheFileMetadata(String dbName, String tblName, String partName, boolean allParts) throws TException {
    return glueMetastoreClientDelegate.cacheFileMetadata(dbName, tblName, partName, allParts);
  }

  @Override
  public List<SQLPrimaryKey> getPrimaryKeys(PrimaryKeysRequest primaryKeysRequest) throws TException {
    return Lists.newArrayList();
  }

  @Override
  public List<SQLForeignKey> getForeignKeys(ForeignKeysRequest foreignKeysRequest) throws TException {
    return Lists.newArrayList();
  }

  @Override
  public void createTableWithConstraints(
      org.apache.hadoop.hive.metastore.api.Table table,
      List<SQLPrimaryKey> primaryKeys,
      List<SQLForeignKey> foreignKeys,
      List<SQLUniqueConstraint> uniqueConstraints,
      List<SQLNotNullConstraint> notNullConstraints,
      List<SQLDefaultConstraint> defaultConstraints,
      List<SQLCheckConstraint> checkConstraints
  ) throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    glueMetastoreClientDelegate.createTableWithConstraints(table, primaryKeys, foreignKeys);
  }

  @Override
  public void dropConstraint(
      String dbName,
      String tblName,
      String constraintName
  ) throws MetaException, NoSuchObjectException, TException {
    glueMetastoreClientDelegate.dropConstraint(dbName, tblName, constraintName);
  }

  @Override
  public void addPrimaryKey(List<SQLPrimaryKey> primaryKeyCols)
      throws MetaException, NoSuchObjectException, TException {
    glueMetastoreClientDelegate.addPrimaryKey(primaryKeyCols);
  }

  @Override
  public void addForeignKey(List<SQLForeignKey> foreignKeyCols)
      throws MetaException, NoSuchObjectException, TException {
    glueMetastoreClientDelegate.addForeignKey(foreignKeyCols);
  }

  @Override
  public ShowCompactResponse showCompactions() throws TException {
    return glueMetastoreClientDelegate.showCompactions();
  }

  @Override
  public void addDynamicPartitions(
      long txnId,
      long writeId,
      String dbName,
      String tblName,
      List<String> partNames
  ) throws TException {
    glueMetastoreClientDelegate.addDynamicPartitions(txnId, dbName, tblName, partNames);
  }

  @Override
  public void addDynamicPartitions(
      long txnId,
      long writeId,
      String dbName,
      String tblName,
      List<String> partNames,
      DataOperationType operationType
  ) throws TException {
    glueMetastoreClientDelegate.addDynamicPartitions(txnId, dbName, tblName, partNames, operationType);
  }

  @Override
  public void insertTable(org.apache.hadoop.hive.metastore.api.Table table, boolean overwrite) throws MetaException {
    glueMetastoreClientDelegate.insertTable(table, overwrite);
  }

  @Override
  public NotificationEventResponse getNextNotification(
      long lastEventId, int maxEvents, NotificationFilter notificationFilter) throws TException {
    return glueMetastoreClientDelegate.getNextNotification(lastEventId, maxEvents, notificationFilter);
  }

  @Override
  public CurrentNotificationEventId getCurrentNotificationEventId() throws TException {
    return glueMetastoreClientDelegate.getCurrentNotificationEventId();
  }

  @Override
  public FireEventResponse fireListenerEvent(FireEventRequest fireEventRequest) throws TException {
    return glueMetastoreClientDelegate.fireListenerEvent(fireEventRequest);
  }

  @Override
  public ShowLocksResponse showLocks() throws TException {
    return glueMetastoreClientDelegate.showLocks();
  }

  @Override
  public ShowLocksResponse showLocks(ShowLocksRequest showLocksRequest) throws TException {
    return glueMetastoreClientDelegate.showLocks(showLocksRequest);
  }

  @Override
  public GetOpenTxnsInfoResponse showTxns() throws TException {
    return glueMetastoreClientDelegate.showTxns();
  }

  @Override
  public boolean tableExists(String databaseName, String tableName) throws MetaException, TException,
        UnknownDBException {
    return glueMetastoreClientDelegate.tableExists(databaseName, tableName);
  }

  @Override
  public void unlock(long lockId) throws NoSuchLockException, TxnOpenException, TException {
    glueMetastoreClientDelegate.unlock(lockId);
  }

  @Override
  public boolean updatePartitionColumnStatistics(org.apache.hadoop.hive.metastore.api.ColumnStatistics columnStatistics)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
      org.apache.hadoop.hive.metastore.api.InvalidInputException {
    return glueMetastoreClientDelegate.updatePartitionColumnStatistics(columnStatistics);
  }

  @Override
  public boolean updateTableColumnStatistics(org.apache.hadoop.hive.metastore.api.ColumnStatistics columnStatistics)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
      org.apache.hadoop.hive.metastore.api.InvalidInputException {
    return glueMetastoreClientDelegate.updateTableColumnStatistics(columnStatistics);
  }

  @Override
  public void validatePartitionNameCharacters(List<String> part_vals) throws TException, MetaException {
      try {
          String partitionValidationRegex = conf.getVar(HiveConf.ConfVars.METASTORE_PARTITION_NAME_WHITELIST_PATTERN);
          Pattern partitionValidationPattern = Strings.isNullOrEmpty(partitionValidationRegex) ? null
                : Pattern.compile(partitionValidationRegex);
          MetaStoreUtils.validatePartitionNameCharacters(part_vals, partitionValidationPattern);
      } catch (Exception e){
          if (e instanceof MetaException) {
              throw (MetaException) e;
          } else {
              throw new MetaException(e.getMessage());
          }
      }
  }

  private Path constructRenamedPath(Path defaultNewPath, Path currentPath) {
      URI currentUri = currentPath.toUri();

      return new Path(currentUri.getScheme(), currentUri.getAuthority(),
            defaultNewPath.toUri().getPath());
  }

  // ========================
  // New Hive 3.1.3 methods
  // ========================

  @Override
  public void createCatalog(Catalog catalog) throws org.apache.hadoop.hive.metastore.api.AlreadyExistsException, InvalidObjectException, MetaException, TException {
    glueMetastoreClientDelegate.createCatalog(catalog);
  }

  @Override
  public void alterCatalog(String s, Catalog catalog) throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
    glueMetastoreClientDelegate.alterCatalog(s, catalog);
  }

  @Override
  public Catalog getCatalog(String s) throws NoSuchObjectException, MetaException, TException {
    return glueMetastoreClientDelegate.getCatalog(s);
  }

  @Override
  public List<String> getCatalogs() throws MetaException, TException {
    return glueMetastoreClientDelegate.getCatalogs();
  }

  @Override
  public void dropCatalog(String s) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    glueMetastoreClientDelegate.dropCatalog(s);
  }

  @Override
  public Database getDatabase(String catalogName, String dbName) throws NoSuchObjectException, MetaException, TException {
    return glueMetastoreClientDelegate.getDatabase(dbName);
  }

  @Override
  public List<String> getDatabases(String catalogName, String dbPattern) throws MetaException, TException {
    return glueMetastoreClientDelegate.getDatabases(dbPattern);
  }

  @Override
  public List<String> getAllDatabases(String catalogName) throws MetaException, TException {
    return getDatabases(".*");
  }

  @Override
  public void alterDatabase(String catalogName, String databaseName, Database database) throws NoSuchObjectException, MetaException, TException {
    glueMetastoreClientDelegate.alterDatabase(databaseName, database);
  }

  @Override
  public void dropDatabase(String catalogName, String dbName, boolean deleteData, boolean ignoreUnknownDb, boolean cascade) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    glueMetastoreClientDelegate.dropDatabase(dbName, deleteData, ignoreUnknownDb, cascade);
  }

  @Override
  public void dropTable(String catName, String dbName, String tableName, boolean deleteData, boolean ignoreUnknownTable, boolean ifPurge) throws MetaException, NoSuchObjectException, TException {
    glueMetastoreClientDelegate.dropTable(dbName, tableName, deleteData, ignoreUnknownTable, ifPurge);
  }

  @Override
  public void truncateTable(String dbName, String tableName, List<String> partNames) throws MetaException, TException {
    throw new UnsupportedOperationException("truncateTable is not supported");
  }

  @Override
  public void truncateTable(String catalogName, String dbName, String tableName, List<String> partNames) throws MetaException, TException {
    throw new UnsupportedOperationException("truncateTable is not supported");
  }

  @Override
  public CmRecycleResponse recycleDirToCmPath(CmRecycleRequest cmRecycleRequest) throws MetaException, TException {
    throw new UnsupportedOperationException("recycleDirToCmPath is not supported");
  }

  @Override
  public boolean tableExists(String catalogName, String databaseName, String tableName) throws MetaException, TException, UnknownDBException {
    return glueMetastoreClientDelegate.tableExists(databaseName, tableName);
  }

  @Override
  public Table getTable(String catalogName, String dbName, String tableName) throws MetaException, TException {
    return glueMetastoreClientDelegate.getTable(dbName, tableName);
  }

  @Override
  public List<String> getTables(String catalogName, String dbname, String tablePattern) throws MetaException, TException, UnknownDBException {
    return glueMetastoreClientDelegate.getTables(dbname, tablePattern);
  }

  @Override
  public List<String> getTables(String catalogName, String dbname, String tablePattern, TableType tableType) throws MetaException, TException, UnknownDBException {
    return glueMetastoreClientDelegate.getTables(dbname, tablePattern, tableType);
  }

  @Override
  public List<String> getAllTables(String catalogName, String dbname) throws MetaException, TException, UnknownDBException {
    return getTables(dbname, ".*");
  }

  @Override
  public List<TableMeta> getTableMeta(String catalogName, String dbPatterns, String tablePatterns, List<String> tableTypes) throws MetaException, TException, UnknownDBException {
    return glueMetastoreClientDelegate.getTableMeta(dbPatterns, tablePatterns, tableTypes);
  }

  @Override
  public List<Table> getTableObjectsByName(String catalogName, String dbName, List<String> tableNames) throws MetaException, InvalidOperationException, UnknownDBException, TException {
    List<Table> hiveTables = Lists.newArrayList();
    for (String tableName : tableNames) {
      hiveTables.add(glueMetastoreClientDelegate.getTable(dbName, tableName));
    }
    return hiveTables;
  }

  @Override
  public List<String> listTableNamesByFilter(String catalogName, String dbName, String filter, int maxTables) throws TException, InvalidOperationException, UnknownDBException {
    return glueMetastoreClientDelegate.listTableNamesByFilter(dbName, filter, (short) maxTables);
  }

  @Override
  public List<String> getMaterializedViewsForRewriting(String dbName) throws MetaException, TException, UnknownDBException {
    return Lists.newArrayList();
  }

  @Override
  public List<String> getMaterializedViewsForRewriting(String catalogName, String dbName) throws MetaException, TException, UnknownDBException {
    return Lists.newArrayList();
  }

  @Override
  public Materialization getMaterializationInvalidationInfo(CreationMetadata creationMetadata, String validTxnList) throws MetaException, InvalidOperationException, UnknownDBException, TException {
    throw new UnsupportedOperationException("getMaterializationInvalidationInfo is not supported");
  }

  @Override
  public void updateCreationMetadata(String dbName, String tableName, CreationMetadata cm) throws MetaException, TException {
    throw new UnsupportedOperationException("updateCreationMetadata is not supported");
  }

  @Override
  public void updateCreationMetadata(String catName, String dbName, String tableName, CreationMetadata cm) throws MetaException, TException {
    throw new UnsupportedOperationException("updateCreationMetadata is not supported");
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition appendPartition(String catalogName, String dbName, String tblName, List<String> values) throws InvalidObjectException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException, MetaException, TException {
    return glueMetastoreClientDelegate.appendPartition(dbName, tblName, values);
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition appendPartition(String catalogName, String dbName, String tblName, String partitionName) throws InvalidObjectException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException, MetaException, TException {
    List<String> partVals = partitionNameToVals(partitionName);
    return glueMetastoreClientDelegate.appendPartition(dbName, tblName, partVals);
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition getPartition(String catalogName, String dbName, String tblName, List<String> values) throws NoSuchObjectException, MetaException, TException {
    return glueMetastoreClientDelegate.getPartition(dbName, tblName, values);
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition getPartition(String catalogName, String dbName, String tblName, String partitionName) throws MetaException, UnknownTableException, NoSuchObjectException, TException {
    return glueMetastoreClientDelegate.getPartition(dbName, tblName, partitionName);
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition getPartitionWithAuthInfo(String catalogName, String databaseName, String tableName, List<String> values, String userName, List<String> groupNames) throws MetaException, UnknownTableException, NoSuchObjectException, TException {
    return getPartitionWithAuthInfo(databaseName, tableName, values, userName, groupNames);
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> getPartitionsByNames(String catalogName, String databaseName, String tableName, List<String> partitionNames) throws NoSuchObjectException, MetaException, TException {
    return glueMetastoreClientDelegate.getPartitionsByNames(databaseName, tableName, partitionNames);
  }

  @Override
  public boolean dropPartition(String catalogName, String dbName, String tblName, List<String> values, boolean deleteData) throws NoSuchObjectException, MetaException, TException {
    return glueMetastoreClientDelegate.dropPartition(dbName, tblName, values, false, deleteData, false);
  }

  @Override
  public boolean dropPartition(String catalogName, String dbName, String tblName, List<String> values, PartitionDropOptions options) throws NoSuchObjectException, MetaException, TException {
    return glueMetastoreClientDelegate.dropPartition(dbName, tblName, values, options.ifExists, options.deleteData, options.purgeData);
  }

  @Override
  public boolean dropPartition(String catalogName, String dbName, String tblName, String partitionName, boolean deleteData) throws NoSuchObjectException, MetaException, TException {
    List<String> values = partitionNameToVals(partitionName);
    return glueMetastoreClientDelegate.dropPartition(dbName, tblName, values, false, deleteData, false);
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> dropPartitions(String catalogName, String dbName, String tblName, List<ObjectPair<Integer, byte[]>> partExprs, PartitionDropOptions options) throws NoSuchObjectException, MetaException, TException {
    return dropPartitions_core(dbName, tblName, partExprs, options.deleteData, options.purgeData);
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitions(String catalogName, String dbName, String tblName, int max) throws NoSuchObjectException, MetaException, TException {
    return listPartitions(dbName, tblName, (short) max);
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitions(String catalogName, String databaseName, String tableName, List<String> values, int max) throws NoSuchObjectException, MetaException, TException {
    return listPartitions(databaseName, tableName, values, (short) max);
  }

  @Override
  public List<String> listPartitionNames(String catalogName, String dbName, String tblName, int maxParts) throws NoSuchObjectException, MetaException, TException {
    return listPartitionNames(dbName, tblName, (short) maxParts);
  }

  @Override
  public List<String> listPartitionNames(String catalogName, String databaseName, String tableName, List<String> values, int max) throws MetaException, TException, NoSuchObjectException {
    return listPartitionNames(databaseName, tableName, values, (short) max);
  }

  @Override
  public PartitionSpecProxy listPartitionSpecs(String catalogName, String dbName, String tblName, int max) throws TException {
    return glueMetastoreClientDelegate.listPartitionSpecs(dbName, tblName, max);
  }

  @Override
  public PartitionSpecProxy listPartitionSpecsByFilter(String catalogName, String dbName, String tblName, String filter, int max) throws MetaException, NoSuchObjectException, TException {
    return glueMetastoreClientDelegate.listPartitionSpecsByFilter(dbName, tblName, filter, max);
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsByFilter(String catalogName, String databaseName, String tableName, String filter, int max) throws MetaException, NoSuchObjectException, TException {
    return listPartitionsByFilter(databaseName, tableName, filter, (short) max);
  }

  @Override
  public boolean listPartitionsByExpr(String catalogName, String databaseName, String tableName, byte[] expr, String defaultPartitionName, int max, List<org.apache.hadoop.hive.metastore.api.Partition> result) throws TException {
    return listPartitionsByExpr(databaseName, tableName, expr, defaultPartitionName, (short) max, result);
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsWithAuthInfo(String catalogName, String database, String table, int maxParts, String user, List<String> groups) throws MetaException, TException, NoSuchObjectException {
    return listPartitionsWithAuthInfo(database, table, (short) maxParts, user, groups);
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsWithAuthInfo(String catalogName, String database, String table, List<String> partVals, int maxParts, String user, List<String> groups) throws MetaException, TException, NoSuchObjectException {
    return listPartitionsWithAuthInfo(database, table, partVals, (short) maxParts, user, groups);
  }

  @Override
  public int getNumPartitionsByFilter(String catalogName, String dbName, String tableName, String filter) throws MetaException, NoSuchObjectException, TException {
    return glueMetastoreClientDelegate.getNumPartitionsByFilter(dbName, tableName, filter);
  }

  @Override
  public void alter_partition(String catalogName, String dbName, String tblName, org.apache.hadoop.hive.metastore.api.Partition partition, EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {
    glueMetastoreClientDelegate.alterPartitions(dbName, tblName, Lists.newArrayList(partition));
  }

  @Override
  public void alter_partitions(String catalogName, String dbName, String tblName, List<org.apache.hadoop.hive.metastore.api.Partition> partitions, EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {
    glueMetastoreClientDelegate.alterPartitions(dbName, tblName, partitions);
  }

  @Override
  public void alter_table(String catalogName, String dbName, String tblName, Table table, EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {
    glueMetastoreClientDelegate.alterTable(dbName, tblName, table, environmentContext);
  }

  @Override
  public void renamePartition(String catalogName, String dbName, String tblName, List<String> partitionValues, org.apache.hadoop.hive.metastore.api.Partition newPartition) throws InvalidOperationException, MetaException, TException {
    renamePartition(dbName, tblName, partitionValues, newPartition);
  }

  @Override
  public void markPartitionForEvent(String catalogName, String dbName, String tblName, Map<String, String> partKVs, PartitionEventType eventType) throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException, UnknownPartitionException, InvalidPartitionException {
    glueMetastoreClientDelegate.markPartitionForEvent(dbName, tblName, partKVs, eventType);
  }

  @Override
  public boolean isPartitionMarkedForEvent(String catalogName, String dbName, String tblName, Map<String, String> partKVs, PartitionEventType eventType) throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException, UnknownPartitionException, InvalidPartitionException {
    return glueMetastoreClientDelegate.isPartitionMarkedForEvent(dbName, tblName, partKVs, eventType);
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Partition exchange_partition(Map<String, String> partitionSpecs, String sourceCat, String sourceDb, String sourceTable, String destCat, String destdb, String destTableName) throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
    return glueMetastoreClientDelegate.exchangePartition(partitionSpecs, sourceDb, sourceTable, destdb, destTableName);
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Partition> exchange_partitions(Map<String, String> partitionSpecs, String sourceCat, String sourceDb, String sourceTbl, String destCat, String destDb, String destTbl) throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
    return glueMetastoreClientDelegate.exchangePartitions(partitionSpecs, sourceDb, sourceTbl, destDb, destTbl);
  }

  @Override
  public ValidWriteIdList getValidWriteIds(String fullTableName) throws TException {
    throw new UnsupportedOperationException("getValidWriteIds is not supported");
  }

  @Override
  public List<TableValidWriteIds> getValidWriteIds(List<String> tablesList, String validTxnList) throws TException {
    throw new UnsupportedOperationException("getValidWriteIds is not supported");
  }

  @Override
  public List<Long> replOpenTxn(String replPolicy, List<Long> srcTxnIds, String user) throws TException {
    throw new UnsupportedOperationException("replOpenTxn is not supported");
  }

  @Override
  public void replRollbackTxn(long l, String s) throws NoSuchTxnException, TException {
    throw new UnsupportedOperationException("replRollbackTxn is not supported");
  }

  @Override
  public void replCommitTxn(long srcTxnid, String replPolicy) throws NoSuchTxnException, TxnAbortedException, TException {
    throw new UnsupportedOperationException("replCommitTxn is not supported");
  }

  @Override
  public long allocateTableWriteId(long txnId, String dbName, String tableName) throws TException {
    throw new UnsupportedOperationException("allocateTableWriteId is not supported");
  }

  @Override
  public void replTableWriteIdState(String validWriteIdList, String dbName, String tableName, List<String> partNames) throws TException {
    throw new UnsupportedOperationException("replTableWriteIdState is not supported");
  }

  @Override
  public List<TxnToWriteId> allocateTableWriteIdsBatch(List<Long> txnIds, String dbName, String tableName) throws TException {
    throw new UnsupportedOperationException("allocateTableWriteIdsBatch is not supported");
  }

  @Override
  public List<TxnToWriteId> replAllocateTableWriteIdsBatch(String dbName, String tableName, String replPolicy, List<TxnToWriteId> srcTxnToWriteIdList) throws TException {
    throw new UnsupportedOperationException("replAllocateTableWriteIdsBatch is not supported");
  }

  @Override
  public NotificationEventsCountResponse getNotificationEventsCount(NotificationEventsCountRequest notificationEventsCountRequest) throws TException {
    throw new UnsupportedOperationException("getNotificationEventsCount is not supported");
  }

  @Override
  public boolean refresh_privileges(HiveObjectRef hiveObjectRef, String s, PrivilegeBag privilegeBag) throws MetaException, TException {
    throw new UnsupportedOperationException("refresh_privileges is not supported");
  }

  @Override
  public List<SQLUniqueConstraint> getUniqueConstraints(UniqueConstraintsRequest uniqueConstraintsRequest) throws MetaException, NoSuchObjectException, TException {
    return Lists.newArrayList();
  }

  @Override
  public List<SQLNotNullConstraint> getNotNullConstraints(NotNullConstraintsRequest notNullConstraintsRequest) throws MetaException, NoSuchObjectException, TException {
    return Lists.newArrayList();
  }

  @Override
  public List<SQLDefaultConstraint> getDefaultConstraints(DefaultConstraintsRequest defaultConstraintsRequest) throws MetaException, NoSuchObjectException, TException {
    return Lists.newArrayList();
  }

  @Override
  public List<SQLCheckConstraint> getCheckConstraints(CheckConstraintsRequest checkConstraintsRequest) throws MetaException, NoSuchObjectException, TException {
    return Lists.newArrayList();
  }

  @Override
  public void addUniqueConstraint(List<SQLUniqueConstraint> uniqueConstraintCols) throws MetaException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException("addUniqueConstraint is not supported");
  }

  @Override
  public void addNotNullConstraint(List<SQLNotNullConstraint> notNullConstraintCols) throws MetaException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException("addNotNullConstraint is not supported");
  }

  @Override
  public void addDefaultConstraint(List<SQLDefaultConstraint> defaultConstraints) throws MetaException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException("addDefaultConstraint is not supported");
  }

  @Override
  public void addCheckConstraint(List<SQLCheckConstraint> checkConstraints) throws MetaException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException("addCheckConstraint is not supported");
  }

  @Override
  public void dropConstraint(String catalogName, String dbName, String tblName, String constraintName) throws MetaException, NoSuchObjectException, TException {
    glueMetastoreClientDelegate.dropConstraint(dbName, tblName, constraintName);
  }

  @Override
  public String getMetastoreDbUuid() throws MetaException, TException {
    throw new UnsupportedOperationException("getMetastoreDbUuid is not supported");
  }

  @Override
  public void createResourcePlan(WMResourcePlan resourcePlan, String copyFromName) throws InvalidObjectException, MetaException, TException {
    throw new UnsupportedOperationException("createResourcePlan is not supported");
  }

  @Override
  public WMFullResourcePlan getResourcePlan(String resourcePlanName) throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException("getResourcePlan is not supported");
  }

  @Override
  public List<WMResourcePlan> getAllResourcePlans() throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException("getAllResourcePlans is not supported");
  }

  @Override
  public void dropResourcePlan(String resourcePlanName) throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException("dropResourcePlan is not supported");
  }

  @Override
  public WMFullResourcePlan alterResourcePlan(String resourcePlanName, WMNullableResourcePlan wmNullableResourcePlan, boolean canActivateDisabled, boolean isForceDeactivate, boolean isReplace) throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
    throw new UnsupportedOperationException("alterResourcePlan is not supported");
  }

  @Override
  public WMFullResourcePlan getActiveResourcePlan() throws MetaException, TException {
    throw new UnsupportedOperationException("getActiveResourcePlan is not supported");
  }

  @Override
  public WMValidateResourcePlanResponse validateResourcePlan(String resourcePlanName) throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
    throw new UnsupportedOperationException("validateResourcePlan is not supported");
  }

  @Override
  public void createWMTrigger(WMTrigger wmTrigger) throws InvalidObjectException, MetaException, TException {
    throw new UnsupportedOperationException("createWMTrigger is not supported");
  }

  @Override
  public void alterWMTrigger(WMTrigger wmTrigger) throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
    throw new UnsupportedOperationException("alterWMTrigger is not supported");
  }

  @Override
  public void dropWMTrigger(String resourcePlanName, String triggerName) throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException("dropWMTrigger is not supported");
  }

  @Override
  public List<WMTrigger> getTriggersForResourcePlan(String resourcePlan) throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException("getTriggersForResourcePlan is not supported");
  }

  @Override
  public void createWMPool(WMPool wmPool) throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
    throw new UnsupportedOperationException("createWMPool is not supported");
  }

  @Override
  public void alterWMPool(WMNullablePool wmNullablePool, String poolPath) throws NoSuchObjectException, InvalidObjectException, TException {
    throw new UnsupportedOperationException("alterWMPool is not supported");
  }

  @Override
  public void dropWMPool(String resourcePlanName, String poolPath) throws TException {
    throw new UnsupportedOperationException("dropWMPool is not supported");
  }

  @Override
  public void createOrUpdateWMMapping(WMMapping wmMapping, boolean isUpdate) throws TException {
    throw new UnsupportedOperationException("createOrUpdateWMMapping is not supported");
  }

  @Override
  public void dropWMMapping(WMMapping wmMapping) throws TException {
    throw new UnsupportedOperationException("dropWMMapping is not supported");
  }

  @Override
  public void createOrDropTriggerToPoolMapping(String resourcePlanName, String triggerName, String poolPath, boolean shouldDrop) throws org.apache.hadoop.hive.metastore.api.AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {
    throw new UnsupportedOperationException("createOrDropTriggerToPoolMapping is not supported");
  }

  @Override
  public void createISchema(ISchema iSchema) throws TException {
    throw new UnsupportedOperationException("createISchema is not supported");
  }

  @Override
  public void alterISchema(String catName, String dbName, String schemaName, ISchema newSchema) throws TException {
    throw new UnsupportedOperationException("alterISchema is not supported");
  }

  @Override
  public ISchema getISchema(String catName, String dbName, String name) throws TException {
    throw new UnsupportedOperationException("getISchema is not supported");
  }

  @Override
  public void dropISchema(String catName, String dbName, String name) throws TException {
    throw new UnsupportedOperationException("dropISchema is not supported");
  }

  @Override
  public void addSchemaVersion(SchemaVersion schemaVersion) throws TException {
    throw new UnsupportedOperationException("addSchemaVersion is not supported");
  }

  @Override
  public SchemaVersion getSchemaVersion(String catName, String dbName, String schemaName, int version) throws TException {
    throw new UnsupportedOperationException("getSchemaVersion is not supported");
  }

  @Override
  public SchemaVersion getSchemaLatestVersion(String catName, String dbName, String schemaName) throws TException {
    throw new UnsupportedOperationException("getSchemaLatestVersion is not supported");
  }

  @Override
  public List<SchemaVersion> getSchemaAllVersions(String catName, String dbName, String schemaName) throws TException {
    throw new UnsupportedOperationException("getSchemaAllVersions is not supported");
  }

  @Override
  public void dropSchemaVersion(String catName, String dbName, String schemaName, int version) throws TException {
    throw new UnsupportedOperationException("dropSchemaVersion is not supported");
  }

  @Override
  public FindSchemasByColsResp getSchemaByCols(FindSchemasByColsRqst findSchemasByColsRqst) throws TException {
    throw new UnsupportedOperationException("getSchemaByCols is not supported");
  }

  @Override
  public void mapSchemaVersionToSerde(String catName, String dbName, String schemaName, int version, String serdeName) throws TException {
    throw new UnsupportedOperationException("mapSchemaVersionToSerde is not supported");
  }

  @Override
  public void setSchemaVersionState(String catName, String dbName, String schemaName, int version, SchemaVersionState state) throws TException {
    throw new UnsupportedOperationException("setSchemaVersionState is not supported");
  }

  @Override
  public void addSerDe(SerDeInfo serDeInfo) throws TException {
    throw new UnsupportedOperationException("addSerDe is not supported");
  }

  @Override
  public SerDeInfo getSerDe(String serDeName) throws TException {
    throw new UnsupportedOperationException("getSerDe is not supported");
  }

  @Override
  public LockResponse lockMaterializationRebuild(String dbName, String tableName, long txnId) throws TException {
    throw new UnsupportedOperationException("lockMaterializationRebuild is not supported");
  }

  @Override
  public boolean heartbeatLockMaterializationRebuild(String dbName, String tableName, long txnId) throws TException {
    throw new UnsupportedOperationException("heartbeatLockMaterializationRebuild is not supported");
  }

  @Override
  public void addRuntimeStat(RuntimeStat runtimeStat) throws TException {
    throw new UnsupportedOperationException("addRuntimeStat is not supported");
  }

  @Override
  public List<RuntimeStat> getRuntimeStats(int maxWeight, int maxCreateTime) throws TException {
    throw new UnsupportedOperationException("getRuntimeStats is not supported");
  }

  @Override
  public List<FieldSchema> getFields(String catalogName, String db, String tableName) throws MetaException, TException, UnknownTableException, UnknownDBException {
    return glueMetastoreClientDelegate.getFields(db, tableName);
  }

  @Override
  public List<FieldSchema> getSchema(String catalogName, String db, String tableName) throws MetaException, TException, UnknownTableException, UnknownDBException {
    return glueMetastoreClientDelegate.getSchema(db, tableName);
  }

  @Override
  public void alterFunction(String catalogName, String dbName, String functionName, Function newFunction) throws InvalidObjectException, MetaException, TException {
    glueMetastoreClientDelegate.alterFunction(dbName, functionName, newFunction);
  }

  @Override
  public void dropFunction(String catalogName, String dbName, String functionName) throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException, TException {
    glueMetastoreClientDelegate.dropFunction(dbName, functionName);
  }

  @Override
  public Function getFunction(String catalogName, String dbName, String functionName) throws MetaException, TException {
    return glueMetastoreClientDelegate.getFunction(dbName, functionName);
  }

  @Override
  public List<String> getFunctions(String catalogName, String dbName, String pattern) throws MetaException, TException {
    return glueMetastoreClientDelegate.getFunctions(dbName, pattern);
  }

  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(String catalogName, String dbName, String tableName, List<String> colNames) throws NoSuchObjectException, MetaException, TException {
    return glueMetastoreClientDelegate.getTableColumnStatistics(dbName, tableName, colNames);
  }

  @Override
  public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String catalogName, String dbName, String tableName, List<String> partitionNames, List<String> columnNames) throws NoSuchObjectException, MetaException, TException {
    return glueMetastoreClientDelegate.getPartitionColumnStatistics(dbName, tableName, partitionNames, columnNames);
  }

  @Override
  public boolean deletePartitionColumnStatistics(String catalogName, String dbName, String tableName, String partName, String colName) throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
    return glueMetastoreClientDelegate.deletePartitionColumnStatistics(dbName, tableName, partName, colName);
  }

  @Override
  public boolean deleteTableColumnStatistics(String catalogName, String dbName, String tableName, String colName) throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
    return glueMetastoreClientDelegate.deleteTableColumnStatistics(dbName, tableName, colName);
  }

  @Override
  public AggrStats getAggrColStatsFor(String catalogName, String dbName, String tblName, List<String> colNames, List<String> partName) throws NoSuchObjectException, MetaException, TException {
    return glueMetastoreClientDelegate.getAggrColStatsFor(dbName, tblName, colNames, partName);
  }

}
