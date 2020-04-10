package com.amazonaws.glue.catalog.metastore.integrationtest;

import com.amazonaws.glue.catalog.converters.CatalogToHiveConverter;
import com.amazonaws.glue.catalog.converters.GlueInputConverter;
import com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient;
import com.amazonaws.glue.catalog.metastore.GlueClientFactory;
import com.amazonaws.glue.catalog.util.GlueTestClientFactory;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static com.amazonaws.glue.catalog.util.TestObjects.getTestDatabase;
import static com.amazonaws.glue.catalog.util.TestObjects.getTestTable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetastoreClientTableIntegrationTest {

  private static AWSGlue glueClient;
  private static IMetaStoreClient metastoreClient;
  private static Warehouse wh;
  private static Database hiveDB;
  private static com.amazonaws.services.glue.model.Database catalogDB;
  private static HiveConf conf;
  private static Path tmpPath;
  private static String invalidTable = "Invalid_Table";
  private static String invalidDatabase = "Invalid_Database";

  private com.amazonaws.services.glue.model.Table catalogTable;
  private Table hiveTable;

  @BeforeClass
  public static void setup() throws MetaException {
    conf = mock(HiveConf.class);
    wh = mock(Warehouse.class);
    tmpPath = new Path("/db");
    when(wh.getDefaultDatabasePath(anyString())).thenReturn(tmpPath);
    when(wh.getDnsPath(any(Path.class))).thenReturn(tmpPath);
    when(wh.isDir(any(Path.class))).thenReturn(true);
    when(conf.get(HiveConf.ConfVars.USERS_IN_ADMIN_ROLE.varname,"")).thenReturn("");

    glueClient = new GlueTestClientFactory().newClient();
    GlueClientFactory clientFactory = mock(GlueClientFactory.class);
    when(clientFactory.newClient()).thenReturn(glueClient);

    metastoreClient = new AWSCatalogMetastoreClient.Builder().withHiveConf(conf).withWarehouse(wh)
            .withClientFactory(clientFactory).build();
    catalogDB = getTestDatabase();
    hiveDB = CatalogToHiveConverter.convertDatabase(catalogDB);
    glueClient.createDatabase(new CreateDatabaseRequest()
      .withDatabaseInput(GlueInputConverter.convertToDatabaseInput(catalogDB)));
  }

  @Before
  public void setUpForClass() {
    catalogTable = getTestTable();
    hiveTable = CatalogToHiveConverter.convertTable(catalogTable, catalogDB.getName());
  }

  @After
  public void cleanUpForClass() {
    String token = null;
    do {
      GetTablesResult result = glueClient.getTables(new GetTablesRequest().withDatabaseName(
          catalogDB.getName()).withNextToken(token));
      token = result.getNextToken();
      for (com.amazonaws.services.glue.model.Table table : result.getTableList()) {
        glueClient.deleteTable(new DeleteTableRequest().withDatabaseName(catalogDB.getName())
                .withName(table.getName()));
      }
    } while (token != null);
  }

  @AfterClass
  public static void clean() {
    glueClient.deleteDatabase(new DeleteDatabaseRequest().withName(catalogDB.getName()));
  }

  @Test
  public void createValidTable() throws TException {
    metastoreClient.createTable(hiveTable);
  }

  @Test (expected = AlreadyExistsException.class)
  public void createDuplicateTable() throws TException {
    metastoreClient.createTable(hiveTable);
    metastoreClient.createTable(hiveTable);
  }
  
  @Test (expected = UnknownDBException.class)
  public void createInvalidTableWithUnknownDatabase() throws Exception {
    hiveTable.setDbName(invalidDatabase);
    metastoreClient.createTable(hiveTable);
  }

  @Test
  public void alterTableValid() throws Exception {
    //TODO: add test for alter Table cascade.
    // if change is related with column and cascade is turned on, it will also change table's partition
    String newType = "boolean";
    Table newHiveTable = CatalogToHiveConverter.convertTable(getTestTable(), hiveTable.getDbName());

    // changing table name is not supported
    newHiveTable.setTableName(hiveTable.getTableName());

    Path oldDBPath = new Path(hiveDB.getLocationUri());
    Path oldTablePath = new Path(hiveTable.getSd().getLocation());
    Path newTablePath = new Path(oldDBPath, newHiveTable.getTableName());

    when(wh.getDatabasePath(hiveDB)).thenReturn(oldDBPath);
    when(wh.getFs(oldTablePath)).thenReturn(new RawLocalFileSystem());
    when(wh.getFs(newTablePath)).thenReturn(new RawLocalFileSystem());

    newHiveTable.setTableType(newType);
    metastoreClient.createTable(hiveTable);

    metastoreClient.alter_table(newHiveTable.getDbName(), hiveTable.getTableName(), newHiveTable);
    Table result = metastoreClient.getTable(hiveDB.getName(), newHiveTable.getTableName());

    assertEquals(newType, result.getTableType());
  }

  @Test (expected = UnknownDBException.class)
  public void alterTableInvalidDB() throws Exception {
    String newType = "newType";
    Table newHiveTable = CatalogToHiveConverter.convertTable(getTestTable(), hiveTable.getDbName());
    newHiveTable.setTableName(hiveTable.getTableName());
    newHiveTable.setTableType(newType);
    metastoreClient.createTable(hiveTable);
    metastoreClient.alter_table(invalidDatabase, hiveTable.getTableName(), newHiveTable);
  }

  @Test (expected = UnknownTableException.class)
  public void alterTableInvalidUnknownTable() throws Exception {
    metastoreClient.alter_table(hiveDB.getName(), hiveTable.getTableName(), hiveTable);
  }

  @Test
  public void checkTableExistsValid() throws TException {
    boolean notExists = metastoreClient.tableExists(hiveDB.getName(), hiveTable.getTableName());
    metastoreClient.createTable(hiveTable);
    boolean exist = metastoreClient.tableExists(hiveDB.getName(), hiveTable.getTableName());

    assertTrue(exist);
    assertFalse(notExists);
  }

  @Test (expected =  UnknownDBException.class)
  public void checkTableExistsInvalid() throws TException {
    metastoreClient.createTable(hiveTable);
    metastoreClient.tableExists(invalidDatabase, hiveTable.getTableName());
  }

  @Test
  public void getFieldsValid() throws Exception{
    int expectedNum = 1;
    metastoreClient.createTable(hiveTable);
    List<FieldSchema> fieldSchemaList = metastoreClient.getFields(hiveDB.getName(), hiveTable.getTableName());
    assertEquals(expectedNum, fieldSchemaList.size());
    assertEquals(hiveTable.getSd().getCols().get(0), fieldSchemaList.get(0));
  }

  @Test (expected = NoSuchObjectException.class)
  public void getFieldInValidWithUnknownDb() throws Exception{
    metastoreClient.createTable(hiveTable);
    metastoreClient.getSchema(invalidDatabase, hiveTable.getTableName());
  }

  @Test (expected = NoSuchObjectException.class)
  public void getFieldInValidWithUnknownTable() throws Exception{
    metastoreClient.createTable(hiveTable);
    metastoreClient.getSchema(hiveDB.getName(), invalidTable);
  }

  @Test
  public void getSchemaValid() throws Exception{
    int expectedNum = 2;
    metastoreClient.createTable(hiveTable);
    List<FieldSchema> schemaList = metastoreClient.getSchema(hiveDB.getName(), hiveTable.getTableName());
    assertEquals(expectedNum, schemaList.size());
    assertTrue(schemaList.containsAll(hiveTable.getPartitionKeys()));
    assertTrue(schemaList.containsAll(hiveTable.getSd().getCols()));
  }

  @Test (expected = NoSuchObjectException.class)
  public void getSchemaInvalidWithUnknownDb() throws Exception {
    metastoreClient.createTable(hiveTable);
    metastoreClient.getSchema(invalidDatabase, hiveTable.getTableName());
  }


  @Test (expected = NoSuchObjectException.class)
  public void getSchemaInvalidWithUnknownTable() throws Exception {
    metastoreClient.createTable(hiveTable);
    metastoreClient.getSchema(hiveDB.getName(), invalidTable);
  }

  @Test
  public void getTableValid() throws Exception {
    metastoreClient.createTable(hiveTable);
    org.apache.hadoop.hive.metastore.api.Table result = metastoreClient.getTable(hiveDB.getName(), hiveTable.getTableName());
    assertTableEqual(result, hiveTable);
  }

  @Test (expected = NoSuchObjectException.class)
  public void getTableInvalidWithUnknownTable() throws Exception {
    metastoreClient.createTable(hiveTable);
    metastoreClient.getTable(hiveDB.getName(), invalidTable);
  }

  @Test (expected = NoSuchObjectException.class)
  public void getTabInvalidWithUnknownDb() throws Exception {
    metastoreClient.createTable(hiveTable);
    metastoreClient.getTable(invalidDatabase, hiveTable.getTableName());
  }

  @Test
  public void getTableObjectsByNameValid() throws TException{
    Table table1 = CatalogToHiveConverter.convertTable(getTestTable(), hiveDB.getName());
    List<String> tableNameList = Lists.newArrayList();
    tableNameList.add(hiveTable.getTableName());
    tableNameList.add(table1.getTableName());
    metastoreClient.createTable(table1);
    metastoreClient.createTable(hiveTable);

    List<org.apache.hadoop.hive.metastore.api.Table> tableList = 
        metastoreClient.getTableObjectsByName(hiveDB.getName(), tableNameList);

    assertEquals(tableNameList.size(), tableList.size());

    for (org.apache.hadoop.hive.metastore.api.Table tbl : tableList) {
      assertTrue(tableNameList.contains(tbl.getTableName()));
    }
  }

  @Test (expected = NoSuchObjectException.class)
  public void getTableObjectsByNameInvalidWithUnknownDb() throws TException {
    Table table1 = CatalogToHiveConverter.convertTable(getTestTable(), hiveDB.getName());
    List<String> tableNameList = Lists.newArrayList();
    tableNameList.add(hiveTable.getTableName());
    tableNameList.add(table1.getTableName());
    metastoreClient.createTable(table1);
    metastoreClient.createTable(hiveTable);

    metastoreClient.getTableObjectsByName(invalidDatabase, tableNameList);
  }

  @Test
  public void dropTableValid() throws Exception {
    metastoreClient.createTable(hiveTable);
    assertTrue(metastoreClient.tableExists(hiveDB.getName(), hiveTable.getTableName()));

    metastoreClient.dropTable(hiveDB.getName(), hiveTable.getTableName(), false, false);
    metastoreClient.dropTable(hiveDB.getName(), invalidTable, false, true);

    assertFalse(metastoreClient.tableExists(hiveDB.getName(), hiveTable.getTableName()));
  }

  @Test (expected = UnknownTableException.class)
  public void dropTableInvalidWithUnKnownTable() throws Exception {
    metastoreClient.dropTable(hiveDB.getName(), invalidTable, false, false);
  }

  @Test (expected = UnknownDBException.class)
  public void dropTableInvalidWithUnknownDb() throws Exception {
    metastoreClient.createTable(hiveTable);
    metastoreClient.dropTable(invalidDatabase, hiveTable.getTableName(),false, false);
  }

  @Test
  public void getTablesValid() throws Exception {
    int expectedNum = 2;
    Table table1 = CatalogToHiveConverter.convertTable(getTestTable(), hiveDB.getName());
    metastoreClient.createTable(hiveTable);
    metastoreClient.createTable(table1);

    List<String> result = metastoreClient.getTables(hiveDB.getName(), ".*");
    assertEquals(expectedNum, result.size());
    assertTrue(result.contains(table1.getTableName()));
    assertTrue(result.contains(hiveTable.getTableName()));
  }

  @Test (expected = NoSuchObjectException.class)
  public void getTablesInvalidWithUnknownDb() throws Exception {
    metastoreClient.createTable(hiveTable);
    metastoreClient.getTables(invalidDatabase, ".*");
  }

  @Test
  public void getAllTablesValid() throws Exception {
    int expectedNum = 2;
    Table table1 = CatalogToHiveConverter.convertTable(getTestTable(), hiveDB.getName());
    metastoreClient.createTable(hiveTable);
    metastoreClient.createTable(table1);

    List<String> result = metastoreClient.getAllTables(hiveDB.getName());
    assertEquals(expectedNum, result.size());
    assertTrue(result.contains(table1.getTableName()));
    assertTrue(result.contains(hiveTable.getTableName()));
  }

  @Test (expected = NoSuchObjectException.class)
  public void getAllTablesInvalidWithUnknownDb() throws Exception {
    metastoreClient.createTable(hiveTable);
    metastoreClient.getAllTables(invalidDatabase);
  }

  /**
   * To avoid asserting flaky variables such as createTime
   */
  private static void assertTableEqual(Table expectedTable, Table actualTable) {
    assertEquals(expectedTable.getTableName(), actualTable.getTableName());
    assertEquals(expectedTable.getDbName(), actualTable.getDbName());
    assertEquals(expectedTable.getOwner(), actualTable.getOwner());
    assertEquals(expectedTable.getSd(), actualTable.getSd());
    assertEquals(expectedTable.getPartitionKeys(), actualTable.getPartitionKeys());
    assertEquals(expectedTable.getParameters(), actualTable.getParameters());
    assertEquals(expectedTable.getTableType(), actualTable.getTableType());
    assertEquals(expectedTable.getViewOriginalText(), actualTable.getViewOriginalText());
    assertEquals(expectedTable.getViewExpandedText(), actualTable.getViewExpandedText());
  }
}
