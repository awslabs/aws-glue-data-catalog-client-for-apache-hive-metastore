package com.amazonaws.glue.catalog.metastore;

import com.amazonaws.glue.catalog.converters.CatalogToHiveConverter;
import com.amazonaws.glue.catalog.converters.GlueInputConverter;
import com.amazonaws.glue.catalog.converters.HiveToCatalogConverter;
import com.amazonaws.glue.catalog.util.ExprBuilder;
import com.amazonaws.glue.catalog.util.ExpressionHelper;
import com.amazonaws.glue.catalog.util.TestObjects;
import com.amazonaws.glue.shims.AwsGlueHiveShims;
import com.amazonaws.glue.shims.ShimsLoader;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.BatchCreatePartitionRequest;
import com.amazonaws.services.glue.model.BatchCreatePartitionResult;
import com.amazonaws.services.glue.model.BatchDeletePartitionRequest;
import com.amazonaws.services.glue.model.BatchDeletePartitionResult;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.CreateUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.CreateUserDefinedFunctionResult;
import com.amazonaws.services.glue.model.DeletePartitionRequest;
import com.amazonaws.services.glue.model.DeletePartitionResult;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.ErrorDetail;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.amazonaws.services.glue.model.GetDatabaseResult;
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
import com.amazonaws.services.glue.model.InternalServiceException;
import com.amazonaws.services.glue.model.InvalidInputException;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.PartitionError;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UpdatePartitionRequest;
import com.amazonaws.services.glue.model.UpdateTableRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;

import static com.amazonaws.glue.catalog.converters.ConverterUtils.catalogTableToString;
import static com.amazonaws.glue.catalog.util.TestObjects.getCatalogTestFunction;
import static com.amazonaws.glue.catalog.util.TestObjects.getTestDatabase;
import static com.amazonaws.glue.catalog.util.TestObjects.getTestHiveIndex;
import static com.amazonaws.glue.catalog.util.TestObjects.getTestPartition;
import static com.amazonaws.glue.catalog.util.TestObjects.getTestTable;
import static com.amazonaws.glue.catalog.util.TestObjects.setIndexParametersForIndexTable;
import static org.apache.hadoop.hive.metastore.HiveMetaStore.PUBLIC;
import static org.apache.hadoop.hive.metastore.MetaStoreUtils.DEFAULT_DATABASE_NAME;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AWSCatalogMetastoreClientTest {

  private static final String INDEX_PREFIX = "index_prefix";

  private AWSGlue glueClient;
  private AWSCatalogMetastoreClient metastoreClient;
  private Warehouse wh;
  private HiveConf conf;
  private GlueClientFactory clientFactory;
  private AWSGlueMetastoreFactory metastoreFactory;
  private final AwsGlueHiveShims hiveShims = ShimsLoader.getHiveShims();

  // Test objects
  private org.apache.hadoop.hive.metastore.api.Database testDB;
  private org.apache.hadoop.hive.metastore.api.Table testTable;
  private org.apache.hadoop.hive.metastore.api.Index testIndex;
  private org.apache.hadoop.hive.metastore.api.Partition testPartition;
  private org.apache.hadoop.hive.metastore.api.Function testFunction;
  private Path defaultWhPath;
  private Path partitionPath;

  @Before
  public void setUp() throws Exception {
    testDB = CatalogToHiveConverter.convertDatabase(getTestDatabase());
    testTable = CatalogToHiveConverter.convertTable(getTestTable(), testDB.getName());
    testIndex = getTestHiveIndex(testDB.getName());
    testPartition = CatalogToHiveConverter.convertPartition(
      getTestPartition(testDB.getName(), testTable.getTableName(), Lists.newArrayList("val1")));
    testFunction = CatalogToHiveConverter.convertFunction(testDB.getName(), getCatalogTestFunction());
    defaultWhPath = new Path("/tmp");
    partitionPath = new Path(testPartition.getSd().getLocation());

    wh = mock(Warehouse.class);
    setupMockWarehouseForPath(defaultWhPath, true, true);
    setupMockWarehouseForPath(partitionPath, false, false);

    conf = spy(new HiveConf());
    conf.setInt(GlueMetastoreClientDelegate.NUM_PARTITION_SEGMENTS_CONF, 1);
    glueClient = spy(AWSGlue.class);
    clientFactory = mock(GlueClientFactory.class);
    metastoreFactory = mock(AWSGlueMetastoreFactory.class);
    when(clientFactory.newClient()).thenReturn(glueClient);
    when(metastoreFactory.newMetastore(conf)).thenReturn(new DefaultAWSGlueMetastore(conf, glueClient));
    metastoreClient = new AWSCatalogMetastoreClient.Builder().withClientFactory(clientFactory)
        .withMetastoreFactory(metastoreFactory).withWarehouse(wh).createDefaults(false).withHiveConf(conf).build();
  }

  private void setupMockWarehouseForPath(Path path, boolean isDir, boolean isDefaultDbPath) throws MetaException {
    when(wh.getDnsPath(path)).thenReturn(path);
    when(wh.isDir(path)).thenReturn(isDir);
    when(wh.mkdirs(path, true)).thenReturn(true);
    if (isDefaultDbPath) {
      when(wh.getDefaultDatabasePath(any(String.class))).thenReturn(path);
    }
  }

  @Test
  public void testDefaultNamespaceCreation() throws Exception {
    doThrow(new EntityNotFoundException("")).when(glueClient).getDatabase(any(GetDatabaseRequest.class));

    when(conf.getVar(conf, ConfVars.USERS_IN_ADMIN_ROLE, "")).thenReturn("");
    metastoreClient = new AWSCatalogMetastoreClient.Builder().withClientFactory(clientFactory)
        .withMetastoreFactory(metastoreFactory).withWarehouse(wh).createDefaults(true).withHiveConf(conf).build();

    verify(glueClient, times(1)).createDatabase(any(CreateDatabaseRequest.class));
    verify(wh, times(1)).getDefaultDatabasePath(DEFAULT_DATABASE_NAME);
    verify(wh, times(1)).isDir(defaultWhPath);
  }

  @Test
  public void testGetAllTable() throws Exception {
    List<Table> result = ImmutableList.of(HiveToCatalogConverter.convertTable(testTable));
    when(glueClient.getTables(new GetTablesRequest().withDatabaseName(testDB.getName()).withExpression(".*")))
            .thenReturn(new GetTablesResult().withTableList(result));
    List<String> tableList = metastoreClient.getAllTables(testDB.getName());
    verify(glueClient).getTables(new GetTablesRequest().withDatabaseName(testDB.getName()).withExpression(".*"));
    assertEquals(Iterables.getOnlyElement(result).getName(), Iterables.getOnlyElement(tableList));
  }

  @Test
  public void testGetFields() throws Exception {
    GetTableRequest request = new GetTableRequest()
        .withDatabaseName(testTable.getDbName())
        .withName(testTable.getTableName());
    when(glueClient.getTable(request)).thenReturn(
        new GetTableResult().withTable(HiveToCatalogConverter.convertTable(testTable)));
    List<FieldSchema> result = metastoreClient.getFields(testDB.getName(), testTable.getTableName());

    verify(glueClient).getTable(request);
    assertThat(result, is(testTable.getSd().getCols()));
  }

  @Test
  public void testGetSchema() throws Exception {
    List<FieldSchema> expectedFieldSchemas = Lists.newArrayList(testTable.getSd().getCols());
    expectedFieldSchemas.addAll(testTable.getPartitionKeys());

    GetTableRequest request = new GetTableRequest()
      .withDatabaseName(testTable.getDbName())
      .withName(testTable.getTableName());
    when(glueClient.getTable(request)).thenReturn(
      new GetTableResult().withTable(HiveToCatalogConverter.convertTable(testTable)));
    List<FieldSchema> result = metastoreClient.getSchema(testTable.getDbName(), testTable.getTableName());

    verify(glueClient).getTable(request);
    assertThat(result, is(expectedFieldSchemas));
  }
  
  @Test
  public void testGetTable() throws Exception {
    when(glueClient.getTable(new GetTableRequest().withDatabaseName(testTable.getDbName())
            .withName(testTable.getTableName()))).thenReturn(new GetTableResult()
            .withTable(HiveToCatalogConverter.convertTable(testTable)));
    org.apache.hadoop.hive.metastore.api.Table result 
      = metastoreClient.getTable(testTable.getDbName(), testTable.getTableName());
    verify(glueClient).getTable(new GetTableRequest().withDatabaseName(testTable.getDbName())
            .withName(testTable.getTableName()));
    assertEquals(testTable, result);
  }

  @Test
  public void testGetTableObjectByName() throws Exception {
    String dbName = testDB.getName();
    List<org.apache.hadoop.hive.metastore.api.Table> expectedHiveTableList = ImmutableList.of(testTable);

    List<String> tableNameList = Lists.newArrayList();
    for (org.apache.hadoop.hive.metastore.api.Table table : expectedHiveTableList) {
      tableNameList.add(table.getTableName());
    }

    when(glueClient.getTable(new GetTableRequest().withDatabaseName(dbName).withName(testTable.getTableName())))
        .thenReturn(new GetTableResult().withTable(HiveToCatalogConverter.convertTable(testTable)));
    List<org.apache.hadoop.hive.metastore.api.Table> result = metastoreClient.getTableObjectsByName(dbName, tableNameList);

    verify(glueClient, times(1)).getTable(any(GetTableRequest.class));
    assertThat(result, is(expectedHiveTableList));
  }

  @Test
  public void testTableExist() throws Exception {
    when(glueClient.getDatabase(new GetDatabaseRequest().withName(testTable.getDbName())))
        .thenReturn(new GetDatabaseResult().withDatabase(HiveToCatalogConverter.convertDatabase(testDB)));
    when(glueClient.getTable(new GetTableRequest().withDatabaseName(testTable.getDbName()).withName(testTable.getTableName())))
        .thenReturn(new GetTableResult().withTable(HiveToCatalogConverter.convertTable(testTable)));
    assertTrue(metastoreClient.tableExists(testTable.getDbName(), testTable.getTableName()));
    verify(glueClient).getTable(any(GetTableRequest.class));
  }

  @Test
  public void testAlterIndex() throws Exception {
    Table catalogIndexTable = HiveToCatalogConverter.convertIndexToTableObject(testIndex);
    testTable.getParameters().put(INDEX_PREFIX + testIndex.getIndexName(), catalogTableToString(catalogIndexTable));

    when(glueClient.getTable(new GetTableRequest().withDatabaseName(testDB.getName()).withName(testTable.getTableName())))
        .thenReturn(new GetTableResult().withTable(HiveToCatalogConverter.convertTable(testTable)));
    when(glueClient.getDatabase(new GetDatabaseRequest().withName(testDB.getName())))
      .thenReturn(new GetDatabaseResult().withDatabase(HiveToCatalogConverter.convertDatabase(testDB)));

    testIndex.setIndexHandlerClass("test_alter");
    Table updatedIndex = HiveToCatalogConverter.convertIndexToTableObject(testIndex);
    TableInput expectedTableInputWithIndex = GlueInputConverter.convertToTableInput(testTable);
    expectedTableInputWithIndex.getParameters().put(INDEX_PREFIX + testIndex.getIndexName(), catalogTableToString(updatedIndex));

    metastoreClient.alter_index(testDB.getName(), testTable.getTableName(), testIndex.getIndexName(), testIndex);

    // verify UpdateRequestTable call is made with expected table input containing new Index
    ArgumentCaptor<UpdateTableRequest> captor = ArgumentCaptor.forClass(UpdateTableRequest.class);
    verify(glueClient, times(1)).updateTable(captor.capture());
    assertEquals(expectedTableInputWithIndex, captor.getValue().getTableInput());
  }

  @Test
  public void testGetIndex() throws Exception {
    Table catalogIndexTableObject = HiveToCatalogConverter.convertIndexToTableObject(testIndex);
    testTable.getParameters().put(INDEX_PREFIX + testIndex.getIndexName(), catalogTableToString(catalogIndexTableObject));

    when(glueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(new GetTableResult().withTable(HiveToCatalogConverter.convertTable(testTable)));
    Index index = metastoreClient.getIndex(testTable.getDbName(), testTable.getTableName(), testIndex.getIndexName());

    verify(glueClient).getTable(any(GetTableRequest.class));
    assertEquals(testIndex, index);
  }

  @Test
  public void testListIndexNames() throws Exception{
    Index testIndex2 = getTestHiveIndex(testDB.getName());
    List<String> expectedIndexNameList = ImmutableList.of(testIndex.getIndexName(), testIndex2.getIndexName());
    List<Index> indexList = ImmutableList.of(testIndex, testIndex2);
    for (Index index : indexList) {
      Table catalogIndex = HiveToCatalogConverter.convertIndexToTableObject(index);
      testTable.getParameters().put(INDEX_PREFIX + index.getIndexName(), catalogTableToString(catalogIndex));
    }

    when(glueClient.getTable(any(GetTableRequest.class)))
      .thenReturn(new GetTableResult().withTable(HiveToCatalogConverter.convertTable(testTable)));
    List<String> result = metastoreClient.listIndexNames(testTable.getDbName(), testTable.getTableName(), (short)2);

    verify(glueClient).getTable(any(GetTableRequest.class));
    assertEquals(expectedIndexNameList.size(), result.size());
    for (String indexName : expectedIndexNameList) {
      assertTrue(result.contains(indexName));
    }
  }

  @Test
  public void testListIndexes() throws Exception {
    Index testIndex2 = getTestHiveIndex(testDB.getName());
    List<Index> expectedIndexList = ImmutableList.of(testIndex, testIndex2);
    for (Index index : expectedIndexList) {
      Table catalogIndex = HiveToCatalogConverter.convertIndexToTableObject(index);
      testTable.getParameters().put(INDEX_PREFIX + index.getIndexName(), catalogTableToString(catalogIndex));
    }

    when(glueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(new GetTableResult().withTable(HiveToCatalogConverter.convertTable(testTable)));
    List<Index> result = metastoreClient.listIndexes(testTable.getDbName(), testTable.getTableName(), (short)2);

    verify(glueClient).getTable(any(GetTableRequest.class));
    assertEquals(expectedIndexList.size(), result.size());
    for (Index index : expectedIndexList) {
      assertTrue(result.contains(index));
    }
  }

  @Test
  public void testCreateIndex() throws Exception {
    Table catalogIndexTable = getTestTable();
    org.apache.hadoop.hive.metastore.api.Table hiveIndexTable = CatalogToHiveConverter.convertTable(catalogIndexTable, testDB.getName());
    testIndex.setOrigTableName(testTable.getTableName());
    testIndex.setIndexTableName(hiveIndexTable.getTableName());

    when(glueClient.getDatabase(new GetDatabaseRequest().withName(testDB.getName())))
        .thenReturn(new GetDatabaseResult().withDatabase(HiveToCatalogConverter.convertDatabase(testDB)));
    when(glueClient.getTable(new GetTableRequest().withDatabaseName(testDB.getName()).withName(testTable.getTableName())))
        .thenReturn(new GetTableResult().withTable(HiveToCatalogConverter.convertTable(testTable)));
    when(glueClient.getTable(new GetTableRequest().withDatabaseName(testDB.getName()).withName(hiveIndexTable.getTableName())))
        .thenThrow(EntityNotFoundException.class);
    when(glueClient.getPartitions(any(GetPartitionsRequest.class)))
        .thenReturn(new GetPartitionsResult().withPartitions(ImmutableList.<Partition>of()));

    metastoreClient.createIndex(testIndex, hiveIndexTable);

    TableInput expectedTableInputWithIndex = GlueInputConverter.convertToTableInput(testTable);
    expectedTableInputWithIndex.getParameters().put(INDEX_PREFIX + testIndex.getIndexName(), catalogTableToString(catalogIndexTable));
    verify(glueClient).createTable(any(CreateTableRequest.class));
    verify(glueClient).updateTable(new UpdateTableRequest().withDatabaseName(testDB.getName()).withTableInput(expectedTableInputWithIndex));
  }

  @Test
  public void testDropIndex() throws Exception {
    Table catalogIndexTable = getTestTable();
    setIndexParametersForIndexTable(catalogIndexTable, testDB.getName(), testTable.getTableName());
    testIndex.setOrigTableName(testTable.getTableName());
    testIndex.setIndexTableName(catalogIndexTable.getName());
    testTable.getParameters().put(INDEX_PREFIX + testIndex.getIndexName(), catalogTableToString(catalogIndexTable));

    when(glueClient.getTable(new GetTableRequest().withDatabaseName(testDB.getName()).withName(testTable.getTableName())))
      .thenReturn(new GetTableResult().withTable(HiveToCatalogConverter.convertTable(testTable)));
    when(glueClient.getTable(new GetTableRequest().withDatabaseName(testDB.getName()).withName(testIndex.getIndexTableName())))
      .thenReturn(new GetTableResult().withTable(catalogIndexTable));
    when(glueClient.getDatabase(new GetDatabaseRequest().withName(testDB.getName())))
      .thenReturn(new GetDatabaseResult().withDatabase(HiveToCatalogConverter.convertDatabase(testDB)));
    when(glueClient.getPartitions(any(GetPartitionsRequest.class)))
      .thenReturn(new GetPartitionsResult().withPartitions(ImmutableList.<Partition>of()));

    metastoreClient.dropIndex(testDB.getName(), testTable.getTableName(), testIndex.getIndexName(), true);

    TableInput expectedTableInput = GlueInputConverter.convertToTableInput(testTable);
    verify(glueClient).updateTable(new UpdateTableRequest().withDatabaseName(testDB.getName()).withTableInput(expectedTableInput));
    verify(glueClient).deleteTable(new DeleteTableRequest().withDatabaseName(testDB.getName()).withName(testIndex.getIndexTableName()));
  }

  @Test
  public void testListPartitionNames() throws Exception {
    List<String> values1 = Arrays.asList("a", "x");
    Partition partition1 = new Partition()
        .withDatabaseName(testDB.getName())
        .withTableName(testTable.getTableName())
        .withValues(values1);

    List<String> values2 = Arrays.asList("a", "y");
    Partition partition2 = new Partition()
        .withDatabaseName(testDB.getName())
        .withTableName(testTable.getTableName())
        .withValues(values2);

    Table table = HiveToCatalogConverter.convertTable(testTable);
    table.setPartitionKeys(Arrays.asList(new Column().withName("foo"), new Column().withName("bar")));

    when(glueClient.getPartitions(any(GetPartitionsRequest.class)))
        .thenReturn(new GetPartitionsResult().withPartitions(Arrays.asList(partition1, partition2)));

    when(glueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(new GetTableResult().withTable(table));

    List<String> partitionNames = metastoreClient.listPartitionNames(
        testDB.getName(), testTable.getTableName(), (short) -1);
    assertThat(partitionNames, containsInAnyOrder("foo=a/bar=x", "foo=a/bar=y"));
  }

  @Test
  public void testListPartitionNamesWithEntityNotFoundException() throws Exception {
    when(glueClient.getPartitions(any(GetPartitionsRequest.class)))
        .thenThrow(EntityNotFoundException.class);
    when(glueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(new GetTableResult().withTable(HiveToCatalogConverter.convertTable(testTable)));
    List<String> partitionNames = metastoreClient.listPartitionNames(
        testDB.getName(), testTable.getTableName(), (short) -1);
    assertTrue(partitionNames.isEmpty());
  }

  @Test
  public void testAddPartitionsCount() throws Exception {
    mockBatchCreatePartitionsSucceed();
    when(glueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(new GetTableResult().withTable(HiveToCatalogConverter.convertTable(testTable)));

    int size = 5;
    List<org.apache.hadoop.hive.metastore.api.Partition> partitions = getTestPartitions(size);
    int created = metastoreClient.add_partitions(partitions);

    verify(glueClient, times(1)).getTable(any(GetTableRequest.class));
    verify(glueClient, times(1)).batchCreatePartition(any(BatchCreatePartitionRequest.class));
    assertEquals(size, created);
    assertDaemonThreadPools();
  }

  private void mockBatchCreatePartitionsSucceed() {
    when(glueClient.batchCreatePartition(any(BatchCreatePartitionRequest.class)))
        .thenReturn(new BatchCreatePartitionResult());
  }

  private List<org.apache.hadoop.hive.metastore.api.Partition> getTestPartitions(int count) {
    List<org.apache.hadoop.hive.metastore.api.Partition> partitions = Lists.newArrayList();
    for (int i = 0; i < count; i++) {
      List<String> values = Arrays.asList("foo" + i);
      Partition partition = TestObjects.getTestPartition(testDB.getName(), testTable.getTableName(), values);
      partitions.add(CatalogToHiveConverter.convertPartition(partition));
    }
    return partitions;
  }

  @Test
  public void testAppendPartitionByName() throws Exception {    
    List<String> values = Arrays.asList("foo");
    when(glueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(new GetTableResult().withTable(HiveToCatalogConverter.convertTable(testTable)));
    Path partLocation = new Path(testTable.getSd().getLocation(), Warehouse
            .makePartName(testTable.getPartitionKeys(), values));
    setupMockWarehouseForPath(partLocation, false, true);
    mockBatchCreatePartitionsSucceed();

    org.apache.hadoop.hive.metastore.api.Partition res = metastoreClient.appendPartition(
        testDB.getName(),
        testTable.getTableName(),
        testTable.getPartitionKeys().get(0).getName() + "=foo");
    assertThat(res.getValues(), is(values));
    assertDaemonThreadPools();
  }
    
  @Test
  public void testDropPartitionUsingName() throws Exception {
    Table table = HiveToCatalogConverter.convertTable(testTable);
    List<String> values = Arrays.asList("foo", "bar");
    Partition partition = new Partition().withDatabaseName(testDB.getName())
        .withTableName(table.getName())
        .withValues(values)
        .withStorageDescriptor(TestObjects.getTestStorageDescriptor());
    when(glueClient.deletePartition(any(DeletePartitionRequest.class))).thenReturn(new DeletePartitionResult());
    when(glueClient.getPartition(any(GetPartitionRequest.class)))
      .thenReturn(new GetPartitionResult().withPartition(partition));
    when(glueClient.getTable(any(GetTableRequest.class)))
      .thenReturn(new GetTableResult().withTable(table));
    when(glueClient.getPartitions(any(GetPartitionsRequest.class)))
      .thenReturn(new GetPartitionsResult().withPartitions(partition));
    // Warehouse is expecting a pattern of /key=val
    String partitionName = table.getPartitionKeys().get(0).getName() + "=foo";
    boolean deleteData = true;
    metastoreClient.dropPartition(testDB.getName(), table.getName(), partitionName, deleteData);
    verify(glueClient).deletePartition(any(DeletePartitionRequest.class));
    verify(glueClient).getPartition(any(GetPartitionRequest.class));
    verify(glueClient).getTable(any(GetTableRequest.class));
    assertDaemonThreadPools();
  }

  @Test
  public void testDropPartitionsEmpty() throws Exception {
    Table table = HiveToCatalogConverter.convertTable(testTable);

    String namespaceName = testDB.getName();
    String tableName = table.getName();

    List<String> values = Arrays.asList("foo", "bar");
    Partition partition = new Partition().withDatabaseName(namespaceName)
        .withTableName(tableName)
        .withValues(values)
        .withStorageDescriptor(TestObjects.getTestStorageDescriptor());

    mockGetPartitionsSuccess(Lists.<Partition>newArrayList());
    mockBatchDeleteSuccess();

    List<org.apache.hadoop.hive.metastore.api.Partition> partitions = metastoreClient.dropPartitions(
        namespaceName, tableName, Lists.newArrayList(getDumbExpression()), false, false, false);

    assertEquals(0, partitions.size());
    assertDaemonThreadPools();
  }

  @Test
  public void testDropPartitionsSucceed() throws Exception {
    Table table = HiveToCatalogConverter.convertTable(testTable);

    String namespaceName = testDB.getName();
    String tableName = table.getName();

    List<String> values1 = Arrays.asList("foo1", "bar1");
    List<String> values2 = Arrays.asList("foo2", "bar2");
    Partition partition1 = getTestPartition(namespaceName, tableName, values1);
    Partition partition2 = getTestPartition(namespaceName, tableName, values2);
    List<Partition> partitions = Lists.newArrayList(partition1, partition2);
    List<org.apache.hadoop.hive.metastore.api.Partition> hivePartitions =
        CatalogToHiveConverter.convertPartitions(partitions);

    mockGetPartitionsSuccess(partitions);
    mockBatchDeleteSuccess();
    when(glueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(new GetTableResult().withTable(HiveToCatalogConverter.convertTable(testTable)));

    List<org.apache.hadoop.hive.metastore.api.Partition> partitionsDropped = metastoreClient.dropPartitions(
        namespaceName, tableName, Lists.newArrayList(getDumbExpression()), true, false, false);

    verify(glueClient, times(1)).batchDeletePartition(any(BatchDeletePartitionRequest.class));
    verify(wh, times(2)).deleteDir(any(Path.class), eq(true), eq(false));
    assertEquals(hivePartitions, partitionsDropped);
    assertDaemonThreadPools();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDropPartitionsDifferentNamespace() throws Exception {
    Table table = HiveToCatalogConverter.convertTable(testTable);

    String namespaceName1 = testDB.getName();
    String tableName1 = table.getName();
    String namespaceName2 = namespaceName1 + ".2";
    String tableName2 = tableName1 + ".2";

    List<String> values1 = Arrays.asList("foo1", "bar1");
    List<String> values2 = Arrays.asList("foo2", "bar2");
    Partition partition1 = getTestPartition(namespaceName1, tableName1, values1);
    Partition partition2 = getTestPartition(namespaceName2, tableName2, values2);
    List<Partition> partitions = Lists.newArrayList(partition1, partition2);

    mockGetPartitionsSuccess(partitions);
    mockBatchDeleteSuccess();
    when(glueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(new GetTableResult().withTable(HiveToCatalogConverter.convertTable(testTable)));

    metastoreClient.dropPartitions(namespaceName1, tableName1,
        Lists.newArrayList(getDumbExpression()), true, false, false);
    assertDaemonThreadPools();
  }

  @Test
  public void testDropPartitionsSucceedTwoPages() throws Exception {
    Table table = HiveToCatalogConverter.convertTable(testTable);
    String namespaceName = testDB.getName();
    String tableName = table.getName();
    final int pageSize = 25;
    final int reqSize = (int) (pageSize * 1.5); // 2 pages
    List<Partition> partitions = Lists.newArrayList();
    for (int i = 0; i < reqSize; i++) {
      partitions.add(TestObjects.getTestPartition(namespaceName, tableName, Arrays.asList("" + i)));
    }
    Set<org.apache.hadoop.hive.metastore.api.Partition> hivePartitions = Sets.newHashSet();
    for (Partition partition : partitions) {
      hivePartitions.add(CatalogToHiveConverter.convertPartition(partition));
    }

    mockGetPartitionsSuccess(partitions);
    mockBatchDeleteSuccess();
    when(glueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(new GetTableResult().withTable(HiveToCatalogConverter.convertTable(testTable)));

    List<org.apache.hadoop.hive.metastore.api.Partition> partitionsDropped = metastoreClient.dropPartitions(
        namespaceName, tableName, Lists.newArrayList(getDumbExpression()), true, false, false);

    verify(glueClient, times(2)).batchDeletePartition(any(BatchDeletePartitionRequest.class));
    verify(wh, times(reqSize)).deleteDir(any(Path.class), eq(true), eq(false));
    assertEquals(reqSize, partitions.size());
    for (org.apache.hadoop.hive.metastore.api.Partition partition : partitionsDropped) {
      assertTrue(hivePartitions.contains(partition));
    }
    assertDaemonThreadPools();
  }

  @Test
  public void testDropPartitionsException() throws Exception {
    Table table = HiveToCatalogConverter.convertTable(testTable);
    String namespaceName = testDB.getName();
    String tableName = table.getName();
    Partition partition = TestObjects.getTestPartition(namespaceName, tableName, Arrays.asList("foo", "bar"));

    mockGetPartitionsSuccess(Lists.newArrayList(partition));
    mockBatchDeleteThrowsException(new NullPointerException("foo error")); // use NPE as a specific RuntimeException
    when(glueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(new GetTableResult().withTable(HiveToCatalogConverter.convertTable(testTable)));

    try {
      metastoreClient.dropPartitions(namespaceName, tableName,
          Lists.newArrayList(getDumbExpression()), true, false, false);
      fail("should throw");
    } catch (TException e) {
      verify(glueClient, times(1)).batchDeletePartition(any(BatchDeletePartitionRequest.class));
      verify(wh, never()).deleteDir(any(Path.class), anyBoolean(), anyBoolean());
      assertThat(e, is(instanceOf(MetaException.class)));
      assertThat(e.getMessage(), is("foo error"));
      assertDaemonThreadPools();
    }
  }

  @Test
  public void testDropPartitionsServiceException() throws Exception {
    Table table = HiveToCatalogConverter.convertTable(testTable);
    String namespaceName = testDB.getName();
    String tableName = table.getName();

    List<String> values1 = Arrays.asList("foo1", "bar1");
    List<String> values2 = Arrays.asList("foo2", "bar2");
    List<String> values3 = Arrays.asList("foo3", "bar3");
    Partition partition1 = getTestPartition(namespaceName, tableName, values1);
    Partition partition2 = getTestPartition(namespaceName, tableName, values2);
    Partition partition3 = getTestPartition(namespaceName, tableName, values3);
    List<Partition> partitions = Lists.newArrayList(partition1, partition2, partition3);
    List<org.apache.hadoop.hive.metastore.api.Partition> hivePartitions =
        CatalogToHiveConverter.convertPartitions(partitions);

    mockGetPartitionsSuccess(partitions);
    mockBatchDeleteThrowsException(new InternalServiceException("InternalServiceException"));
    when(glueClient.getPartition(any(GetPartitionRequest.class)))
        .thenReturn(new GetPartitionResult().withPartition(partition1))
        .thenThrow(new EntityNotFoundException("EntityNotFoundException"))
        .thenThrow(new NullPointerException("NullPointerException"));
    when(glueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(new GetTableResult().withTable(HiveToCatalogConverter.convertTable(testTable)));

    try {
      metastoreClient.dropPartitions(namespaceName, tableName,
          Lists.newArrayList(getDumbExpression()), true, false, false);
      fail("should throw");
    } catch (TException e) {
      verify(glueClient, times(1)).batchDeletePartition(any(BatchDeletePartitionRequest.class));
      verify(glueClient, times(3)).getPartition(any(GetPartitionRequest.class));
      verify(wh, times(1)).deleteDir(any(Path.class), eq(true), eq(false));
      assertThat(e, is(instanceOf(MetaException.class)));
      assertThat(e.getMessage(), containsString("InternalServiceException"));
      assertDaemonThreadPools();
    }
  }

  @Test
  public void testDropPartitionsClientException() throws Exception {
    Table table = HiveToCatalogConverter.convertTable(testTable);
    String namespaceName = testDB.getName();
    String tableName = table.getName();
    Partition partition = TestObjects.getTestPartition(namespaceName, tableName, Arrays.asList("foo", "bar"));

    mockGetPartitionsSuccess(Lists.newArrayList(partition));
    mockBatchDeleteThrowsException(new InvalidInputException("InvalidInputException"));
    when(glueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(new GetTableResult().withTable(HiveToCatalogConverter.convertTable(testTable)));

    try {
      metastoreClient.dropPartitions(namespaceName, tableName,
          Lists.newArrayList(getDumbExpression()), true, false, false);
      fail("should throw");
    } catch (TException e) {
      verify(glueClient, times(1)).batchDeletePartition(any(BatchDeletePartitionRequest.class));
      verify(glueClient, never()).getPartition(any(GetPartitionRequest.class));
      verify(wh, never()).deleteDir(any(Path.class), anyBoolean(), anyBoolean());
      assertThat(e, is(instanceOf(InvalidObjectException.class)));
      assertThat(e.getMessage(), containsString("InvalidInputException"));
    }
    assertDaemonThreadPools();
  }

  @Test
  public void testDropPartitionsExceptionTwoPages() throws Exception {
    Table table = HiveToCatalogConverter.convertTable(testTable);
    String namespaceName = testDB.getName();
    String tableName = table.getName();
    final int pageSize = 25;
    final int reqSize = (int) (pageSize * 1.5); // 2 pages
    List<Partition> partitions = Lists.newArrayList();
    for (int i = 0; i < reqSize; i++) {
      partitions.add(TestObjects.getTestPartition(namespaceName, tableName, Arrays.asList("" + i)));
    }

    mockGetPartitionsSuccess(partitions);
    when(glueClient.batchDeletePartition(Mockito.any(BatchDeletePartitionRequest.class)))
        .thenThrow(new InternalServiceException("InternalServiceException"));
    when(glueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(new GetTableResult().withTable(HiveToCatalogConverter.convertTable(testTable)));
    when(glueClient.getPartition(any(GetPartitionRequest.class)))
        .thenReturn(new GetPartitionResult().withPartition(partitions.get(0)))
        .thenThrow(new EntityNotFoundException("EntityNotFoundException"))
        .thenThrow(new NullPointerException("NullPointerException"));

    try {
      metastoreClient.dropPartitions(namespaceName, tableName,
          Lists.newArrayList(getDumbExpression()), true, false, false);
      fail("should throw");
    } catch (TException e) {
      verify(glueClient, times(2)).batchDeletePartition(any(BatchDeletePartitionRequest.class));
      verify(glueClient, times(reqSize)).getPartition(any(GetPartitionRequest.class));
      verify(wh, times(1)).deleteDir(any(Path.class), eq(true), eq(false));
      assertThat(e, is(instanceOf(MetaException.class)));
      assertThat(e.getMessage(), containsString("InternalServiceException"));
      assertDaemonThreadPools();
    }
  }

  @Test
  public void testDropPartitionsPartialFailure() throws Exception {
    Table table = HiveToCatalogConverter.convertTable(testTable);
    String namespaceName = testDB.getName();
    String tableName = table.getName();
    List<String> values1 = Arrays.asList("foo1", "bar1");
    List<String> values2 = Arrays.asList("foo2", "bar2");
    Partition partition1 = getTestPartition(namespaceName, tableName, values1);
    Partition partition2 = getTestPartition(namespaceName, tableName, values2);
    List<Partition> partitions = Lists.newArrayList(partition1, partition2);

    mockGetPartitionsSuccess(partitions);
    mockBatchDeleteWithFailures(Lists.newArrayList(getPartitionError(values1,
        new EntityNotFoundException("EntityNotFoundException"))));
    when(glueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(new GetTableResult().withTable(HiveToCatalogConverter.convertTable(testTable)));

    try {
      metastoreClient.dropPartitions(namespaceName, tableName,
          Lists.newArrayList(getDumbExpression()), true, false, false);
      fail("should throw");
    } catch (TException e) {
      verify(glueClient, times(1)).batchDeletePartition(any(BatchDeletePartitionRequest.class));
      verify(glueClient, never()).getPartition(any(GetPartitionRequest.class));
      verify(wh, times(1)).deleteDir(any(Path.class), eq(true), eq(false));
      assertThat(e, is(instanceOf(NoSuchObjectException.class)));
      assertThat(e.getMessage(), containsString("EntityNotFoundException"));
      assertDaemonThreadPools();
    }
  }

  private ObjectPair<Integer, byte[]> getDumbExpression() throws Exception {
    ExprNodeGenericFuncDesc expr = new ExprBuilder("fooTable")
        .val("value").strCol("key").pred("=", 2).build();
    byte[] payload = hiveShims.getSerializeExpression(expr);
    return new ObjectPair<>(0, payload);
  }

  private void mockGetPartitionsSuccess(List<Partition> partitions) {
    when(glueClient.getPartitions(any(GetPartitionsRequest.class)))
        .thenReturn(new GetPartitionsResult().withPartitions(partitions).withNextToken(null));
  }

  private void mockBatchDeleteSuccess() {
    Mockito.when(glueClient.batchDeletePartition(Mockito.any(BatchDeletePartitionRequest.class)))
        .thenReturn(new BatchDeletePartitionResult());
  }

  private void mockBatchDeleteWithFailures(Collection<PartitionError> errors) {
    Mockito.when(glueClient.batchDeletePartition(Mockito.any(BatchDeletePartitionRequest.class)))
        .thenReturn(new BatchDeletePartitionResult().withErrors(errors));
  }

  private void mockBatchDeleteThrowsException(Exception e) {
    Mockito.when(glueClient.batchDeletePartition(Mockito.any(BatchDeletePartitionRequest.class))).thenThrow(e);
  }

  private PartitionError getPartitionError(List<String> values, Exception exception) {
    return new PartitionError()
        .withPartitionValues(values)
        .withErrorDetail(new ErrorDetail()
            .withErrorCode(exception.getClass().getSimpleName())
            .withErrorMessage(exception.getMessage()));
  }
  
  @Test
  public void testListPartitionsWithAuthInfo() throws Exception {
    String dbName = "default";
    String tblName = "foo";
    List<String> values = ImmutableList.of("1970-01-01 12%3A34%3A56");
    Partition partition = TestObjects.getTestPartition(dbName, tblName, values);

    String expression = ExpressionHelper.buildExpressionFromPartialSpecification(testTable, values);
    GetPartitionsRequest expectedRequest = new GetPartitionsRequest()
      .withDatabaseName(dbName)
      .withTableName(tblName)
      .withExpression(expression);

    when(glueClient.getDatabase(any(GetDatabaseRequest.class)))
    .thenReturn(new GetDatabaseResult().withDatabase(HiveToCatalogConverter.convertDatabase(testDB)));
    when(glueClient.getTable(any(GetTableRequest.class)))
    .thenReturn(new GetTableResult().withTable(HiveToCatalogConverter.convertTable(testTable)));
    when(glueClient.getPartitions(expectedRequest))
      .thenReturn(new GetPartitionsResult().withPartitions(ImmutableList.of(partition)));
    
    metastoreClient.listPartitionsWithAuthInfo(dbName, tblName, values, (short) 1, null, null);

    // Ensure the call reaches here despite the exception thrown by getPrincipalPrivilegeSet
    verify(glueClient).getPartitions(expectedRequest);
  }
  
  @Test
  public void testRenamePartitionForHiveManagedTable() throws Exception {
    String dbName = testDB.getName();
    String tblName = testTable.getTableName();
    List<String> partitionValues = testPartition.getValues();
    Partition catalogPartition = HiveToCatalogConverter.convertPartition(testPartition);
    
    Partition newPartition = new Partition()
                                  .withDatabaseName(dbName).withTableName(tblName)
                                  .withValues(Lists.newArrayList("newval"))
                                  .withStorageDescriptor(catalogPartition.getStorageDescriptor());
    
    Path srcPath = new Path(testPartition.getSd().getLocation());
    Path expectedDestPath = new Path("/db/tbl/" ,
        Warehouse.makePartName(testTable.getPartitionKeys(), newPartition.getValues()));
        
    when(glueClient.getDatabase(any(GetDatabaseRequest.class)))
      .thenReturn(new GetDatabaseResult().withDatabase(HiveToCatalogConverter.convertDatabase(testDB)));
    when(glueClient.getTable(any(GetTableRequest.class)))
    .thenReturn(new GetTableResult().withTable(HiveToCatalogConverter.convertTable(testTable)));
    when(glueClient.getPartition(any(GetPartitionRequest.class)))
      .thenReturn(new GetPartitionResult().withPartition(catalogPartition));

    FileSystem fs = mock(FileSystem.class);
    when(fs.getUri()).thenReturn(new URI("s3://bucket"));
    when(hiveShims.getDefaultTablePath(any(org.apache.hadoop.hive.metastore.api.Database.class), anyString(), wh))
        .thenReturn(new Path("/db/tbl"));
    when(wh.getFs(any(Path.class))).thenReturn(fs);
    when(fs.exists(srcPath)).thenReturn(true);
    when(fs.exists(expectedDestPath)).thenReturn(false);
    when(wh.mkdirs(expectedDestPath.getParent(), true)).thenReturn(true);       
    
    metastoreClient.renamePartition(dbName, tblName, partitionValues,
        CatalogToHiveConverter.convertPartition(newPartition));
    
    // Verify catalog service is called , dest path's parent dirs are created and dfs rename is done
    verify(glueClient, times(1)).updatePartition(any(UpdatePartitionRequest.class));
    verify(wh).mkdirs(expectedDestPath.getParent(), true);
    verify(wh).renameDir(srcPath, expectedDestPath, true);
  }
  
  @Test
  public void testRenamePartitionRevertWhenMetastoreoperationFails() throws Exception {
    String dbName = testDB.getName();
    String tblName = testTable.getTableName();
    List<String> partitionValues = testPartition.getValues();
    Partition catalogPartition = HiveToCatalogConverter.convertPartition(testPartition);
    
    Partition newPartition = new Partition()
                                .withDatabaseName(dbName).withTableName(tblName)
                                .withValues(Lists.newArrayList("newval"))
                                .withStorageDescriptor(catalogPartition.getStorageDescriptor());
    
    Path srcPath = new Path(testPartition.getSd().getLocation());
    Path expectedDestPath = new Path("/db/tbl/" ,
        Warehouse.makePartName(testTable.getPartitionKeys(), newPartition.getValues()));
        
    when(glueClient.getDatabase(any(GetDatabaseRequest.class)))
      .thenReturn(new GetDatabaseResult().withDatabase(HiveToCatalogConverter.convertDatabase(testDB)));
    when(glueClient.getTable(any(GetTableRequest.class)))
    .thenReturn(new GetTableResult().withTable(HiveToCatalogConverter.convertTable(testTable)));
    when(glueClient.getPartition(any(GetPartitionRequest.class)))
      .thenReturn(new GetPartitionResult().withPartition(catalogPartition));

    FileSystem fs = mock(FileSystem.class);
    when(fs.getUri()).thenReturn(new URI("s3://bucket"));
    when(hiveShims.getDefaultTablePath(any(org.apache.hadoop.hive.metastore.api.Database.class), anyString(), wh))
        .thenReturn(new Path("/db/tbl"));
    when(wh.getFs(any(Path.class))).thenReturn(fs);
    when(fs.exists(srcPath)).thenReturn(true);
    when(fs.exists(expectedDestPath)).thenReturn(false);
    
    // Fail directory creation
    when(wh.mkdirs(expectedDestPath.getParent(), true)).thenReturn(false);
    
    boolean exceptionThrown = false;
    try {
      metastoreClient.renamePartition(dbName, tblName, partitionValues,
          CatalogToHiveConverter.convertPartition(newPartition));
    } catch(InvalidOperationException e) {
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);
    
    // Verify catalog service is called , dest path's parent dirs are created and dfs rename is done
    verify(glueClient, times(2)).updatePartition(any(UpdatePartitionRequest.class));
  }
  
  @Test(expected=InvalidOperationException.class)
  public void testRenamePartitionDestinationAlreadyExists() throws Exception {
    String dbName = testDB.getName();
    String tblName = testTable.getTableName();
    List<String> partitionValues = testPartition.getValues();
    Partition catalogPartition = HiveToCatalogConverter.convertPartition(testPartition);
    
    Partition newPartition = new Partition()
                                  .withDatabaseName(dbName).withTableName(tblName)
                                  .withValues(Lists.newArrayList("newval"))
                                  .withStorageDescriptor(catalogPartition.getStorageDescriptor());
    
    Path srcPath = new Path(testPartition.getSd().getLocation());
    Path expectedDestPath = new Path("/db/tbl/" , Warehouse.makePartName(testTable.getPartitionKeys(),
        newPartition.getValues()));
        
    when(glueClient.getDatabase(any(GetDatabaseRequest.class)))
      .thenReturn(new GetDatabaseResult().withDatabase(HiveToCatalogConverter.convertDatabase(testDB)));
    when(glueClient.getTable(any(GetTableRequest.class)))
    .thenReturn(new GetTableResult().withTable(HiveToCatalogConverter.convertTable(testTable)));
    when(glueClient.getPartition(any(GetPartitionRequest.class)))
      .thenReturn(new GetPartitionResult().withPartition(catalogPartition));
    
    
    FileSystem fs = mock(FileSystem.class);
    when(fs.getUri()).thenReturn(new URI("s3://bucket"));
    when(hiveShims.getDefaultTablePath(any(org.apache.hadoop.hive.metastore.api.Database.class), anyString(), wh))
        .thenReturn(new Path("/db/tbl"));
    when(wh.getFs(any(Path.class))).thenReturn(fs);
    when(fs.exists(srcPath)).thenReturn(true);
    when(fs.exists(expectedDestPath)).thenReturn(true);
        
    metastoreClient.renamePartition(dbName, tblName, partitionValues,
        CatalogToHiveConverter.convertPartition(newPartition));
  }
  
  @Test
  public void testRenamePartitionForExternalTable() throws Exception {
    String dbName = testDB.getName();
    Table externalTable = getTestTable();
    externalTable.setTableType(TableType.EXTERNAL_TABLE.name());   
    
    StorageDescriptor sd = HiveToCatalogConverter.convertStorageDescriptor(testPartition.getSd());
    
    Partition oldPartition = new Partition()
        .withDatabaseName(dbName).withTableName(externalTable.getName())
        .withValues(Lists.newArrayList("oldval")).withStorageDescriptor(sd);
    
    Partition newPartition = new Partition()
                                      .withDatabaseName(dbName).withTableName(externalTable.getName())
                                      .withValues(Lists.newArrayList("newval")).withStorageDescriptor(sd);

    when(glueClient.getDatabase(any(GetDatabaseRequest.class)))
      .thenReturn(new GetDatabaseResult().withDatabase(HiveToCatalogConverter.convertDatabase(testDB)));
    when(glueClient.getTable(any(GetTableRequest.class)))
    .thenReturn(new GetTableResult().withTable(externalTable));
    when(glueClient.getPartition(any(GetPartitionRequest.class)))
      .thenReturn(new GetPartitionResult().withPartition(oldPartition));

    metastoreClient.renamePartition(dbName, externalTable.getName(), oldPartition.getValues(),
        CatalogToHiveConverter.convertPartition(newPartition));
    
    // Verify catalog service is called and no interactions with wh
    verify(glueClient, times(1)).updatePartition(any(UpdatePartitionRequest.class));
    verifyNoMoreInteractions(wh);
  }
  
  @Test(expected=InvalidOperationException.class)
  public void testRenamePartitionForUnknownTable() throws Exception {
    String dbName = testDB.getName();
    Table externalTable = getTestTable();
    externalTable.setTableType(TableType.EXTERNAL_TABLE.name());   
    
    StorageDescriptor sd = HiveToCatalogConverter.convertStorageDescriptor(testPartition.getSd());
    
    Partition oldPartition = new Partition()
        .withDatabaseName(dbName).withTableName(externalTable.getName())
        .withValues(Lists.newArrayList("oldval")).withStorageDescriptor(sd);
    
    Partition newPartition = new Partition()
                                      .withDatabaseName(dbName).withTableName(externalTable.getName())
                                      .withValues(Lists.newArrayList("newval")).withStorageDescriptor(sd);    
        
    when(glueClient.getDatabase(any(GetDatabaseRequest.class)))
      .thenReturn(new GetDatabaseResult().withDatabase(HiveToCatalogConverter.convertDatabase(testDB)));
    doThrow(EntityNotFoundException.class).when(glueClient).getTable(any(GetTableRequest.class));
    
    when(glueClient.getPartition(any(GetPartitionRequest.class)))
      .thenReturn(new GetPartitionResult().withPartition(oldPartition));     
    
    metastoreClient.renamePartition(dbName, externalTable.getName(), oldPartition.getValues(),
        CatalogToHiveConverter.convertPartition(newPartition));
  }
  
  @Test(expected=InvalidOperationException.class)
  public void testRenamePartitionForUnknownPartition() throws Exception {
    String dbName = testDB.getName();
    Table externalTable = getTestTable();
    externalTable.setTableType(TableType.EXTERNAL_TABLE.name());   
    
    StorageDescriptor sd = HiveToCatalogConverter.convertStorageDescriptor(testPartition.getSd());
    
    Partition oldPartition = new Partition()
        .withDatabaseName(dbName).withTableName(externalTable.getName())
        .withValues(Lists.newArrayList("oldval")).withStorageDescriptor(sd);
    
    Partition newPartition = new Partition()
                                      .withDatabaseName(dbName).withTableName(externalTable.getName())
                                      .withValues(Lists.newArrayList("newval")).withStorageDescriptor(sd);

    when(glueClient.getDatabase(any(GetDatabaseRequest.class)))
      .thenReturn(new GetDatabaseResult().withDatabase(HiveToCatalogConverter.convertDatabase(testDB)));
    when(glueClient.getTable(any(GetTableRequest.class)))
    .thenReturn(new GetTableResult().withTable(externalTable));
    doThrow(EntityNotFoundException.class).when(glueClient).getPartition(any(GetPartitionRequest.class));
    
    metastoreClient.renamePartition(dbName, externalTable.getName(), oldPartition.getValues(),
        CatalogToHiveConverter.convertPartition(newPartition));
  }
  
  @Test(expected=InvalidOperationException.class)
  public void testRenamePartitionForInvalidSD() throws Exception {
    String dbName = testDB.getName();
    Table externalTable = getTestTable();
    externalTable.setTableType(TableType.EXTERNAL_TABLE.name());   
    
    StorageDescriptor sd = HiveToCatalogConverter.convertStorageDescriptor(testPartition.getSd());
    
    Partition oldPartition = new Partition()
        .withDatabaseName(dbName).withTableName(externalTable.getName())
        .withValues(Lists.newArrayList("oldval")).withStorageDescriptor(null);
    
    Partition newPartition = new Partition()
                                      .withDatabaseName(dbName).withTableName(externalTable.getName())
                                      .withValues(Lists.newArrayList("newval")).withStorageDescriptor(sd);

    when(glueClient.getDatabase(any(GetDatabaseRequest.class)))
      .thenReturn(new GetDatabaseResult().withDatabase(HiveToCatalogConverter.convertDatabase(testDB)));
    when(glueClient.getTable(any(GetTableRequest.class)))
    .thenReturn(new GetTableResult().withTable(externalTable));
    when(glueClient.getPartition(any(GetPartitionRequest.class)))
      .thenReturn(new GetPartitionResult().withPartition(oldPartition));

    metastoreClient.renamePartition(dbName, externalTable.getName(), oldPartition.getValues(),
        CatalogToHiveConverter.convertPartition(newPartition));
  }

  @Test
  public void testValidatePartitionNameValid() throws Exception{
    List<String> partitionValue = Lists.newArrayList("partitionValue");
    when(conf.getVar(HiveConf.ConfVars.METASTORE_PARTITION_NAME_WHITELIST_PATTERN)).thenReturn("[a-zA-Z0-9]+");
    metastoreClient.validatePartitionNameCharacters(partitionValue);
  }

  @Test (expected = MetaException.class)
  public void testValidatePartitionNameInvalid() throws Exception{
    List<String> partitionValue = Lists.newArrayList(".partitionValue");
    when(conf.getVar(HiveConf.ConfVars.METASTORE_PARTITION_NAME_WHITELIST_PATTERN)).thenReturn("[a-zA-Z0-9]+");
    metastoreClient.validatePartitionNameCharacters(partitionValue);
  }
  
  @Test
  public void testListPartitionsByExpression() throws Exception {
    String dbName = testDB.getName();
    Table table = getTestTable();
    ExprBuilder e = new ExprBuilder(table.getName()).val("test").strCol("location").pred("=", 2);
    ExprNodeGenericFuncDesc exprTree = e.build();
    byte[] payload = hiveShims.getSerializeExpression(exprTree);
    List<org.apache.hadoop.hive.metastore.api.Partition> results = new ArrayList<>();

    Partition newPartition = new Partition()
      .withDatabaseName(dbName).withTableName(table.getName())
      .withValues(Lists.newArrayList("newval")).withStorageDescriptor(null);
    when(glueClient.getPartitions(any(GetPartitionsRequest.class)))
            .thenReturn(new GetPartitionsResult().withPartitions(newPartition));
    boolean status = metastoreClient.listPartitionsByExpr(dbName,table.getName(),payload,null,(short)5,results);
    assertFalse(status);
  }

  @Test
  public void testListPartitionByFilter() throws Exception {
    String dbName = testDB.getName();
    Table table = getTestTable();
    String filter = "part = \"aaa\"";
    String convertedFilter = "part = \'aaa\'";

    when(glueClient.getPartitions(any(GetPartitionsRequest.class)))
      .thenReturn(new GetPartitionsResult().withPartitions(HiveToCatalogConverter.convertPartition(testPartition)));
    List<org.apache.hadoop.hive.metastore.api.Partition> results =
      metastoreClient.listPartitionsByFilter(dbName, table.getName(), filter, (short)-1);

    assertEquals(testPartition, results.get(0));
  }

  @Test
  public void testCreateFunctionValid() throws Exception {
    when(glueClient.createUserDefinedFunction(any(CreateUserDefinedFunctionRequest.class))).thenReturn(new CreateUserDefinedFunctionResult());
    metastoreClient.createFunction(testFunction);
  }

  @Test (expected =  NoSuchObjectException.class)
  public void testCreateFunctionInValidWithUnknownNamespace() throws Exception {
    when(glueClient.createUserDefinedFunction(any(CreateUserDefinedFunctionRequest.class)))
            .thenThrow(new EntityNotFoundException(""));
    metastoreClient.createFunction(testFunction);
  }

  @Test (expected = org.apache.hadoop.hive.metastore.api.AlreadyExistsException.class)
  public void testCreateFunctionInValidWithDuplicateFunction() throws Exception {
    when(glueClient.createUserDefinedFunction(any(CreateUserDefinedFunctionRequest.class)))
            .thenThrow(new com.amazonaws.services.glue.model.AlreadyExistsException(""));
    metastoreClient.createFunction(testFunction);
  }

  @Test (expected = MetaException.class)
  public void testCreateFunctionInvalidWithMetaException() throws Exception {
    when(glueClient.createUserDefinedFunction(any(CreateUserDefinedFunctionRequest.class)))
            .thenThrow(new InternalServiceException(""));
    metastoreClient.createFunction(testFunction);
  }

  @Test
  public void testGetFunctionValid() throws Exception {
    when(glueClient.getUserDefinedFunction(any(GetUserDefinedFunctionRequest.class)))
            .thenReturn(new GetUserDefinedFunctionResult().withUserDefinedFunction(
                HiveToCatalogConverter.convertFunction(testFunction)));
    Function result = metastoreClient.getFunction(testDB.getName(), testFunction.getFunctionName());
    assertEquals(testFunction, result);
  }

  @Test (expected = NoSuchObjectException.class)
  public void testGetFunctionInValidWithEntityNotFoundException() throws Exception {
    when(glueClient.getUserDefinedFunction(any(GetUserDefinedFunctionRequest.class)))
            .thenThrow(new EntityNotFoundException(""));
    metastoreClient.getFunction(testDB.getName(), testFunction.getFunctionName());
  }

  @Test (expected = MetaException.class)
  public void testGetFunctionInValidWithMetaException() throws Exception {
    when(glueClient.getUserDefinedFunction(any(GetUserDefinedFunctionRequest.class)))
            .thenThrow(new InternalServiceException(""));
   metastoreClient.getFunction(testDB.getName(), testFunction.getFunctionName());
  }

  @Test
  public void testListFunctionsValid() throws Exception {
    List<Function> functions = Lists.newArrayList(testFunction);
    when(glueClient.getUserDefinedFunctions(any(GetUserDefinedFunctionsRequest.class)))
            .thenReturn(new GetUserDefinedFunctionsResult().withUserDefinedFunctions(
                HiveToCatalogConverter.convertFunction(testFunction)));
    List<String> result = metastoreClient.getFunctions(testDB.getName(), ".*");
    assertEquals(functions.size(), result.size());
    assertEquals(functions.get(0).getFunctionName(), result.get(0));
  }

  @Test (expected = NoSuchObjectException.class)
  public void testListFunctionsInValidWithNoSuchObjectException() throws Exception {
    when(glueClient.getUserDefinedFunctions(any(GetUserDefinedFunctionsRequest.class)))
            .thenThrow(new EntityNotFoundException(""));
    metastoreClient.getFunctions(testDB.getName(), ".*");
  }

  @Test (expected = MetaException.class)
  public void testListFunctionsInValidWithMetaException() throws Exception {
    when(glueClient.getUserDefinedFunctions(any(GetUserDefinedFunctionsRequest.class)))
            .thenThrow(new InternalServiceException(""));
    metastoreClient.getFunctions(testDB.getName(), ".*");
  }

  @Test
  public void testListFunctionsValidWithPagination() throws Exception {
    when(glueClient.getUserDefinedFunctions(any(GetUserDefinedFunctionsRequest.class)))
        .thenReturn(new GetUserDefinedFunctionsResult()
            .withUserDefinedFunctions(HiveToCatalogConverter.convertFunction(testFunction))
            .withNextToken("token"))
        .thenReturn(new GetUserDefinedFunctionsResult().withUserDefinedFunctions(getCatalogTestFunction()));
    List<String> result = metastoreClient.getFunctions(testDB.getName(), ".*");

    int expectedNumFunctions = 2;
    assertEquals(expectedNumFunctions, result.size());
    verify(glueClient, times(expectedNumFunctions)).getUserDefinedFunctions(any(GetUserDefinedFunctionsRequest.class));
  }

  @Test
  public void testGetMetaConfDefault() throws TException {
    HiveConf.ConfVars metaConfVar = ConfVars.METASTORE_TRY_DIRECT_SQL;
    String expected = metaConfVar.getDefaultValue();
    assertEquals(expected, metastoreClient.getMetaConf(metaConfVar.toString()));
  }

  // Roles & Privilege
  @Test(expected=UnsupportedOperationException.class)
  public void testGrantPublicRole() throws Exception {
    metastoreClient.grant_role("public", "user",
        org.apache.hadoop.hive.metastore.api.PrincipalType.USER, "grantor",
        org.apache.hadoop.hive.metastore.api.PrincipalType.ROLE, true);
  }

  @Test(expected=UnsupportedOperationException.class)
  public void testRevokeRole() throws Exception {
    metastoreClient.revoke_role("role", "user",
        org.apache.hadoop.hive.metastore.api.PrincipalType.USER, true);
  }

  @Test(expected=UnsupportedOperationException.class)
  public void testCreateRole() throws Exception {
    metastoreClient.create_role(new org.apache.hadoop.hive.metastore.api.Role(
        "role", (int) (new Date().getTime() / 1000), "owner"));
  }

  @Test(expected=UnsupportedOperationException.class)
  public void testCreatePublicRole() throws Exception {
    metastoreClient.create_role(new org.apache.hadoop.hive.metastore.api.Role(
        "public", (int) (new Date().getTime() / 1000), "owner"));
  }

  @Test(expected=UnsupportedOperationException.class)
  public void testDropRole() throws Exception {
    metastoreClient.drop_role("role");
  }

  @Test(expected=UnsupportedOperationException.class)
  public void testDropPublicRole() throws Exception {
    metastoreClient.drop_role("public");
  }

  @Test(expected=UnsupportedOperationException.class)
  public void testDropAdminRole() throws Exception {
    metastoreClient.drop_role("admin");
  }

  @Test(expected=UnsupportedOperationException.class)
  public void testListRolesWithRolePrincipalType() throws Exception {
    metastoreClient.list_roles("user", PrincipalType.ROLE);
  }

  @Test
  public void testListRoles() throws Exception {
    List<Role> results = metastoreClient.list_roles("user", PrincipalType.USER);

    assertEquals(results.size(), 1);
    assertEquals(results.get(0).getRoleName(), PUBLIC);
  }

  @Test
  public void testListRoleNames() throws Exception {
    List<String> results = metastoreClient.listRoleNames();

    assertEquals(results.size(), 1);
    assertEquals(results.get(0), PUBLIC);
  }

  @Test(expected=UnsupportedOperationException.class)
  public void testGetPrincipalsInRole() throws Exception {
    metastoreClient.get_principals_in_role(
        new org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest("role"));
  }

  @Test(expected=UnsupportedOperationException.class)
  public void testRoleGrantsForPrincipal() throws Exception {
    metastoreClient.get_role_grants_for_principal(
        new org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest("user",
            org.apache.hadoop.hive.metastore.api.PrincipalType.USER));
  }

  @Test(expected=UnsupportedOperationException.class)
  public void testGrantRole() throws Exception {
    metastoreClient.grant_role("role", "user",
        org.apache.hadoop.hive.metastore.api.PrincipalType.USER, "grantor",
        org.apache.hadoop.hive.metastore.api.PrincipalType.ROLE, true);
  }

  @Test(expected=UnsupportedOperationException.class)
  public void testGrantPrivileges() throws Exception {
    metastoreClient.grant_privileges(TestObjects.getPrivilegeBag());
  }

  @Test(expected=UnsupportedOperationException.class)
  public void testRevokePrivileges() throws Exception {
    metastoreClient.revoke_privileges(TestObjects.getPrivilegeBag(), false);
  }

  @Test(expected=UnsupportedOperationException.class)
  public void testListPrivileges() throws Exception {
    String principal = "user1";
    org.apache.hadoop.hive.metastore.api.PrincipalType principalType =
        org.apache.hadoop.hive.metastore.api.PrincipalType.USER;

    metastoreClient.list_privileges(principal, principalType, TestObjects.getHiveObjectRef());
  }

  @Test
  public void testGetPrincipalPrivilegeSet() throws Exception {
    String user = "user1";
    List<String> groupList = ImmutableList.of();
    org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet privilegeSet = metastoreClient
        .get_privilege_set(TestObjects.getHiveObjectRef(), user, groupList);

    assertThat(privilegeSet, is(nullValue()));
  }

  @Test(expected=UnsupportedOperationException.class)
  public void testGrantPrivilegesThrowingMetaException() throws Exception {
    metastoreClient.grant_privileges(TestObjects.getPrivilegeBag());
  }

  // Statistics
  @Test(expected=UnsupportedOperationException.class)
  public void testDeletePartitionColumnStatisticsValid() throws Exception {
    String databaseName = "database-name";
    String tableName = "table-name";
    String partitionName = "A=a/B=b";
    String columnName = "column-name";

    metastoreClient.deletePartitionColumnStatistics(databaseName, tableName, partitionName, columnName);
  }

  @Test(expected=UnsupportedOperationException.class)
  public void testDeleteTableColumnStatistics() throws Exception {
    String databaseName = "database-name";
    String tableName = "table-name";
    String columnName = "column-name";

    metastoreClient.deleteTableColumnStatistics(databaseName, tableName, columnName);
  }

  @Test(expected=UnsupportedOperationException.class)
  public void testGetPartitionColumnStatisticsValid() throws Exception {
    String databaseName = "database-name";
    String tableName = "table-name";
    List<String> partitionNames = Arrays.asList("A=a/B=b", "A=x/B=y");
    List<String> columnNames = Arrays.asList("decimal-column", "string-column");

    metastoreClient.getPartitionColumnStatistics(databaseName, tableName, partitionNames, columnNames);
  }

  @Test(expected=UnsupportedOperationException.class)
  public void testGetTableColumnStatistics() throws Exception {
    String databaseName = "database-name";
    String tableName = "table-name";
    List<String> columnNames = Arrays.asList("decimal-column", "string-column");

    metastoreClient.getTableColumnStatistics(databaseName, tableName, columnNames);
  }

  @Test(expected=UnsupportedOperationException.class)
  public void testUpdatePartitionColumnStatistics() throws Exception {
    org.apache.hadoop.hive.metastore.api.ColumnStatistics columnStatistics =
        TestObjects.getHivePartitionColumnStatistics();

    metastoreClient.updatePartitionColumnStatistics(columnStatistics);
  }

  @Test(expected=UnsupportedOperationException.class)
  public void testUpdateTableColumnStatistics() throws Exception {
    org.apache.hadoop.hive.metastore.api.ColumnStatistics columnStatistics =
        TestObjects.getHiveTableColumnStatistics();

    metastoreClient.updateTableColumnStatistics(columnStatistics);
  }

  private void assertDaemonThreadPools() {
    String threadNameDeletePrefix =
            AWSCatalogMetastoreClient.BATCH_DELETE_PARTITIONS_THREAD_POOL_NAME_FORMAT.substring(
                    0,
                    AWSCatalogMetastoreClient.BATCH_DELETE_PARTITIONS_THREAD_POOL_NAME_FORMAT.indexOf('%'));
    String threadNameCreatePrefix =
            GlueMetastoreClientDelegate.GLUE_METASTORE_DELEGATE_THREADPOOL_NAME_FORMAT.substring(
                    0,
                    GlueMetastoreClientDelegate.GLUE_METASTORE_DELEGATE_THREADPOOL_NAME_FORMAT.indexOf('%'));
    for (Thread thread : Thread.getAllStackTraces().keySet()) {
      String threadName = thread.getName();
      if (threadName != null &&
        (threadName.startsWith(threadNameDeletePrefix) || threadName.startsWith(threadNameCreatePrefix))) {
        assertTrue(thread.isDaemon());
      }
    }
  }

}
