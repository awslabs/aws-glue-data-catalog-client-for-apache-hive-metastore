package com.amazonaws.glue.catalog.metastore.integrationtest;

import com.amazonaws.glue.catalog.converters.CatalogToHiveConverter;
import com.amazonaws.glue.catalog.converters.GlueInputConverter;
import com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient;
import com.amazonaws.glue.catalog.metastore.GlueClientFactory;
import com.amazonaws.glue.catalog.util.GlueTestClientFactory;
import com.amazonaws.glue.catalog.util.TestObjects;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.UpdateTableRequest;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.amazonaws.glue.catalog.metastore.GlueMetastoreClientDelegate.INDEX_PREFIX;
import static com.amazonaws.glue.catalog.util.TestObjects.getTestDatabase;
import static com.amazonaws.glue.catalog.util.TestObjects.getTestTable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetastoreClientIndexIntegrationTest {

  private static AWSGlue glueClient;
  private static IMetaStoreClient metastoreClient;
  private static Warehouse wh;
  private static Database hiveDB;
  private static Table hiveTable;
  private static com.amazonaws.services.glue.model.Database catalogDB;
  private static com.amazonaws.services.glue.model.Table catalogTable;
  private static HiveConf conf;
  private static Path tmpPath;

  private static String invalidTable = "Invalid_Table";
  private static String invalidDatabase = "Invalid_Database";
  private static String invalidIndex = "Invalid_Index";

  private com.amazonaws.services.glue.model.Table catalogIndexTable;
  private Index hiveIndex;
  private Table hiveIndexTable;

  @BeforeClass
  public static void setup() throws MetaException {
    conf = mock(HiveConf.class);
    wh = mock(Warehouse.class);
    tmpPath = new Path("/db");
    when(wh.getDefaultDatabasePath(anyString())).thenReturn(tmpPath);
    when(wh.getDnsPath(any(Path.class))).thenReturn(tmpPath);
    when(wh.isDir(any(Path.class))).thenReturn(true);
    when(wh.getDatabasePath(any(Database.class))).thenReturn(tmpPath);
    when(conf.get(HiveConf.ConfVars.USERS_IN_ADMIN_ROLE.varname,"")).thenReturn("");

    glueClient = new GlueTestClientFactory().newClient();
    GlueClientFactory clientFactory = mock(GlueClientFactory.class);
    when(clientFactory.newClient()).thenReturn(glueClient);

    metastoreClient = new AWSCatalogMetastoreClient.Builder().withHiveConf(conf).withWarehouse(wh)
            .withClientFactory(clientFactory).build();
    catalogDB = getTestDatabase();
    catalogTable = getTestTable();
    hiveDB = CatalogToHiveConverter.convertDatabase(catalogDB);
    hiveTable = CatalogToHiveConverter.convertTable(catalogTable, catalogDB.getName());

    glueClient.createDatabase(new CreateDatabaseRequest()
      .withDatabaseInput(GlueInputConverter.convertToDatabaseInput(catalogDB)));
    glueClient.createTable(new CreateTableRequest()
      .withDatabaseName(catalogDB.getName())
      .withTableInput(GlueInputConverter.convertToTableInput(catalogTable)));
  }

  @Before
  public void setUpForTest() {
    catalogIndexTable = getTestTable();
    hiveIndexTable = CatalogToHiveConverter.convertTable(catalogIndexTable, hiveDB.getName());

    hiveIndex = TestObjects.getTestHiveIndex(catalogDB.getName());
    hiveIndex.setOrigTableName(hiveTable.getTableName());
    hiveIndex.setIndexTableName(hiveIndexTable.getTableName());
  }

  @After
  public void cleanUpForTest() {
    com.amazonaws.services.glue.model.Table catalogTable = glueClient.getTable(new GetTableRequest()
            .withDatabaseName(catalogDB.getName()).withName(hiveTable.getTableName())).getTable();
    Map<String, String> parameters = catalogTable.getParameters();
    List<String> to_delete = new ArrayList<>();
    for(String key : parameters.keySet()) {
      if(key.startsWith(INDEX_PREFIX)) {
        to_delete.add(key);
      }
    }

    for(String to_delete_key : to_delete) {
      parameters.remove(to_delete_key);
    }

    glueClient.updateTable(new UpdateTableRequest().withDatabaseName(hiveDB.getName())
            .withTableInput(GlueInputConverter.convertToTableInput(catalogTable)));
  }

  @AfterClass
  public static void cleanUp() {
    List<com.amazonaws.services.glue.model.Table> table_to_delete = new ArrayList<>();
    String token = null;
    do {
      GetTablesResult result = glueClient.getTables(new GetTablesRequest().withDatabaseName(
          catalogDB.getName()).withNextToken(token));
      table_to_delete.addAll(result.getTableList());
      token = result.getNextToken();
    } while(token != null);
    for(com.amazonaws.services.glue.model.Table table : table_to_delete) {
      glueClient.deleteTable(new DeleteTableRequest().withDatabaseName(catalogDB.getName()).withName(table.getName()));
    }
    glueClient.deleteDatabase(new DeleteDatabaseRequest().withName(catalogDB.getName()));
  }

  @Test
  public void createValidIndex() throws TException {
    metastoreClient.createIndex(hiveIndex, hiveIndexTable);
    assertTrue(indexExist(hiveDB.getName(), hiveTable.getTableName(), hiveIndex.getIndexName()));
  }

  @Test (expected = AlreadyExistsException.class)
  public void createDuplicateIndex() throws TException {
    assertFalse(indexExist(hiveDB.getName(), hiveTable.getTableName(), hiveIndex.getIndexName()));
    metastoreClient.createIndex(hiveIndex, hiveIndexTable);
    assertTrue(indexExist(hiveDB.getName(), hiveTable.getTableName(), hiveIndex.getIndexName()));

    Table hiveIndexTable2 = CatalogToHiveConverter.convertTable(getTestTable(), hiveDB.getName());
    hiveIndex.setIndexTableName(hiveIndexTable2.getTableName());

    metastoreClient.createIndex(hiveIndex, hiveIndexTable2);
  }

  @Test (expected = UnknownDBException.class)
  public void createIndexWithUnknownDatabase() throws TException {
    hiveIndex.setDbName(invalidDatabase);
    Table invalidHiveTable = CatalogToHiveConverter.convertTable(getTestTable(), invalidDatabase);
    metastoreClient.createIndex(hiveIndex, invalidHiveTable);
  }

  @Test (expected = NoSuchObjectException.class)
  public void createIndexWithUnknownTable() throws TException {
    hiveIndex.setOrigTableName(invalidTable);
    Table invalidHiveTable = CatalogToHiveConverter.convertTable(getTestTable(), hiveDB.getName());
    metastoreClient.createIndex(hiveIndex, invalidHiveTable);
  }


  @Test
  public void alterValidIndex() throws TException {
    metastoreClient.createIndex(hiveIndex, hiveIndexTable);

    Index newIndex = TestObjects.getTestHiveIndex(hiveDB.getName());
    newIndex.setIndexTableName(hiveIndexTable.getTableName());
    newIndex.setOrigTableName(hiveTable.getTableName());

    metastoreClient.alter_index(hiveDB.getName(), hiveTable.getTableName(), hiveIndex.getIndexName(), newIndex);

    assertFalse(indexExist(hiveDB.getName(), hiveTable.getTableName(), hiveIndex.getIndexName()));
    assertTrue(indexExist(hiveDB.getName(), hiveTable.getTableName(), newIndex.getIndexName()));
  }

  @Test (expected = NoSuchObjectException.class)
  public void alterIndexInvalidWithUnknownDatabase() throws TException {
    metastoreClient.createIndex(hiveIndex, hiveIndexTable);

    Index newIndex = TestObjects.getTestHiveIndex(hiveDB.getName());
    metastoreClient.alter_index(invalidDatabase, hiveTable.getTableName(), hiveIndex.getIndexName(), newIndex);
  }

  @Test (expected = NoSuchObjectException.class)
  public void alterIndexInvalidWithUnknownTable() throws TException {
    metastoreClient.createIndex(hiveIndex, hiveIndexTable);

    Index newIndex = TestObjects.getTestHiveIndex(hiveDB.getName());
    newIndex.setOrigTableName(hiveTable.getTableName());
    newIndex.setIndexTableName(hiveIndexTable.getTableName());

    metastoreClient.alter_index(hiveDB.getName(), invalidTable, hiveIndex.getIndexName(), newIndex);
  }

  @Test
  public void dropIndexValid() throws TException{
    //TODO: update test after implementation of partition.
    // When cascade is turn on, it needs to drop index Table and its partitions.

    metastoreClient.createIndex(hiveIndex, hiveIndexTable);
    assertTrue(indexExist(hiveDB.getName(), hiveTable.getTableName(), hiveIndex.getIndexName()));

    metastoreClient.dropIndex(hiveDB.getName(), hiveTable.getTableName(), hiveIndex.getIndexName(), false);
    assertFalse(indexExist(hiveDB.getName(), hiveTable.getTableName(), hiveIndex.getIndexTableName()));
  }

  @Test (expected =  NoSuchObjectException.class)
  public void dropInvalidIndex() throws TException{
    assertFalse(indexExist(hiveDB.getName(), hiveTable.getTableName(), invalidIndex));
    metastoreClient.dropIndex(hiveDB.getName(), hiveTable.getTableName(), invalidIndex, false);
  }

  @Test (expected = NoSuchObjectException.class)
  public void dropIndexInvalidWithUnknownTable() throws TException{
    metastoreClient.createIndex(hiveIndex, hiveIndexTable);
    assertTrue(indexExist(hiveDB.getName(), hiveTable.getTableName(), hiveIndex.getIndexName()));

    metastoreClient.dropIndex(hiveDB.getName(),invalidTable, hiveIndex.getIndexName(), false);
  }

  @Test (expected = NoSuchObjectException.class)
  public void dropIndexInvalidWithUnknownDataBase() throws TException{
    metastoreClient.createIndex(hiveIndex, hiveIndexTable);
    assertTrue(indexExist(hiveDB.getName(), hiveTable.getTableName(), hiveIndex.getIndexName()));

    metastoreClient.dropIndex(invalidDatabase,hiveTable.getTableName(), hiveIndex.getIndexName(), false);
  }

  @Test
  public void listIndexesValid() throws TException {
    Index index2 = TestObjects.getTestHiveIndex(hiveDB.getName());
    Index index3 = TestObjects.getTestHiveIndex(hiveDB.getName());

    Table hiveIndexTable2 = CatalogToHiveConverter.convertTable(getTestTable(), hiveDB.getName());
    Table hiveIndexTable3 = CatalogToHiveConverter.convertTable(getTestTable(), hiveDB.getName());

    index2.setOrigTableName(hiveTable.getTableName());
    index2.setIndexTableName(hiveIndexTable2.getTableName());
    index3.setOrigTableName(hiveTable.getTableName());
    index3.setIndexTableName(hiveIndexTable3.getTableName());

    metastoreClient.createIndex(hiveIndex, hiveIndexTable);
    metastoreClient.createIndex(index2, hiveIndexTable2);
    metastoreClient.createIndex(index3, hiveIndexTable3);
    List<Index> indexList = new ArrayList<>();
    indexList.add(hiveIndex);
    indexList.add(index3);
    indexList.add(index2);

    List<Index> result = metastoreClient.listIndexes(hiveDB.getName(), hiveTable.getTableName(), (short) 2);
    assertTrue(result.containsAll(indexList));
  }

  @Test (expected = NoSuchObjectException.class)
  public void listIndexesInvalidWithUnknownTable() throws TException {
    Index index2 = TestObjects.getTestHiveIndex(hiveDB.getName());
    Index index3 = TestObjects.getTestHiveIndex(hiveDB.getName());

    Table hiveIndexTable2 = CatalogToHiveConverter.convertTable(getTestTable(), hiveDB.getName());
    Table hiveIndexTable3 = CatalogToHiveConverter.convertTable(getTestTable(), hiveDB.getName());

    index2.setOrigTableName(hiveTable.getTableName());
    index2.setIndexTableName(hiveIndexTable2.getTableName());
    index3.setOrigTableName(hiveTable.getTableName());
    index3.setIndexTableName(hiveIndexTable3.getTableName());

    metastoreClient.createIndex(hiveIndex, hiveIndexTable);
    metastoreClient.createIndex(index2, hiveIndexTable2);
    metastoreClient.createIndex(index3, hiveIndexTable3);
    metastoreClient.listIndexes(hiveDB.getName(), invalidTable, (short) 2);
  }

  @Test (expected = NoSuchObjectException.class)
  public void listIndexesInvalidWithUnknownDb() throws TException {
    Index index2 = TestObjects.getTestHiveIndex(hiveDB.getName());
    Index index3 = TestObjects.getTestHiveIndex(hiveDB.getName());

    Table hiveIndexTable2 = CatalogToHiveConverter.convertTable(getTestTable(), hiveDB.getName());
    Table hiveIndexTable3 = CatalogToHiveConverter.convertTable(getTestTable(), hiveDB.getName());

    index2.setOrigTableName(hiveTable.getTableName());
    index2.setIndexTableName(hiveIndexTable2.getTableName());
    index3.setOrigTableName(hiveTable.getTableName());
    index3.setIndexTableName(hiveIndexTable3.getTableName());

    metastoreClient.createIndex(hiveIndex, hiveIndexTable);
    metastoreClient.createIndex(index2, hiveIndexTable2);
    metastoreClient.createIndex(index3, hiveIndexTable3);
    metastoreClient.listIndexes(invalidDatabase, hiveTable.getTableName(), (short) 2);
  }

  @Test
  public void getIndexValid() throws TException {
    metastoreClient.createIndex(hiveIndex, hiveIndexTable);
    Index result = metastoreClient.getIndex(hiveDB.getName(), hiveTable.getTableName(), hiveIndex.getIndexName());
    assertEquals(hiveIndex, result);
  }

  @Test (expected = NoSuchObjectException.class)
  public void getUnknownIndex() throws TException {
    metastoreClient.createIndex(hiveIndex, hiveIndexTable);
    metastoreClient.getIndex(hiveDB.getName(), hiveTable.getTableName(), invalidIndex);
  }

  @Test (expected = NoSuchObjectException.class)
  public void getIndexInvalidWithUnknownTable() throws TException{
    metastoreClient.createIndex(hiveIndex, hiveIndexTable);
    metastoreClient.getIndex(hiveDB.getName(), invalidTable, hiveIndex.getIndexName());
  }

  @Test (expected = NoSuchObjectException.class)
  public void getIndexInvalidWithUnknownDb() throws TException {
    metastoreClient.createIndex(hiveIndex, hiveIndexTable);
    metastoreClient.getIndex(invalidDatabase, hiveTable.getTableName(), hiveIndex.getIndexName());
  }

  @Test
  public void listIndexNamesValid() throws TException {
    Index index2 = TestObjects.getTestHiveIndex(hiveDB.getName());
    Index index3 = TestObjects.getTestHiveIndex(hiveDB.getName());

    Table hiveIndexTable2 = CatalogToHiveConverter.convertTable(getTestTable(), hiveDB.getName());
    Table hiveIndexTable3 = CatalogToHiveConverter.convertTable(getTestTable(), hiveDB.getName());

    index2.setOrigTableName(hiveTable.getTableName());
    index2.setIndexTableName(hiveIndexTable2.getTableName());
    index3.setOrigTableName(hiveTable.getTableName());
    index3.setIndexTableName(hiveIndexTable3.getTableName());

    metastoreClient.createIndex(hiveIndex, hiveIndexTable);
    metastoreClient.createIndex(index2, hiveIndexTable2);
    metastoreClient.createIndex(index3, hiveIndexTable3);
    List<String> indexNameList = new ArrayList<>();
    indexNameList.add(hiveIndex.getIndexName());
    indexNameList.add(index2.getIndexName());
    indexNameList.add(index3.getIndexName());

    List<String> result = metastoreClient.listIndexNames(hiveDB.getName(), hiveTable.getTableName(), (short) 2);

    assertTrue(result.containsAll(indexNameList));
  }

  @Test (expected = NoSuchObjectException.class)
  public void listIndexNameInvalidWithUnknownTable() throws TException {
    Index index2 = TestObjects.getTestHiveIndex(hiveDB.getName());
    Index index3 = TestObjects.getTestHiveIndex(hiveDB.getName());

    Table hiveIndexTable2 = CatalogToHiveConverter.convertTable(getTestTable(), hiveDB.getName());
    Table hiveIndexTable3 = CatalogToHiveConverter.convertTable(getTestTable(), hiveDB.getName());

    index2.setOrigTableName(hiveTable.getTableName());
    index2.setIndexTableName(hiveIndexTable2.getTableName());
    index3.setOrigTableName(hiveTable.getTableName());
    index3.setIndexTableName(hiveIndexTable3.getTableName());

    metastoreClient.createIndex(hiveIndex, hiveIndexTable);
    metastoreClient.createIndex(index2, hiveIndexTable2);
    metastoreClient.createIndex(index3, hiveIndexTable3);

    metastoreClient.listIndexNames(hiveDB.getName(), invalidTable, (short) 2);
  }

  @Test (expected = NoSuchObjectException.class)
  public void listIndexNameInvalidWithUnknownDb() throws TException {
    Index index2 = TestObjects.getTestHiveIndex(hiveDB.getName());
    Index index3 = TestObjects.getTestHiveIndex(hiveDB.getName());

    Table hiveIndexTable2 = CatalogToHiveConverter.convertTable(getTestTable(), hiveDB.getName());
    Table hiveIndexTable3 = CatalogToHiveConverter.convertTable(getTestTable(), hiveDB.getName());

    index2.setOrigTableName(hiveTable.getTableName());
    index2.setIndexTableName(hiveIndexTable2.getTableName());
    index3.setOrigTableName(hiveTable.getTableName());
    index3.setIndexTableName(hiveIndexTable3.getTableName());

    metastoreClient.createIndex(hiveIndex, hiveIndexTable);
    metastoreClient.createIndex(index2, hiveIndexTable2);
    metastoreClient.createIndex(index3, hiveIndexTable3);

    metastoreClient.listIndexNames(invalidDatabase, hiveTable.getTableName(), (short) 2);
  }
  
  private boolean indexExist(final String dbName, final String tableName, final String indexName) throws TException{
    List<String> indexes = metastoreClient.listIndexNames(dbName, tableName, Short.MAX_VALUE);
    return indexes.contains(indexName);
  }

}
