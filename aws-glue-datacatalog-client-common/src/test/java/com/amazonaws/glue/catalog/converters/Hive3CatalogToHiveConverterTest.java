package com.amazonaws.glue.catalog.converters;

import com.amazonaws.glue.catalog.util.TestObjects;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.UserDefinedFunction;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.junit.Test;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;
import static org.junit.Assert.assertEquals;

public class Hive3CatalogToHiveConverterTest {

  private static final String TEST_DB_NAME = "testDb";
  private static final String TEST_TBL_NAME = "testTbl";
  private final CatalogToHiveConverter catalogToHiveConverter = new Hive3CatalogToHiveConverter();

  @Test
  public void testDatabaseCatalogName() {
    Database catalogDb = TestObjects.getTestDatabase();
    org.apache.hadoop.hive.metastore.api.Database hiveDatabase = catalogToHiveConverter
        .convertDatabase(catalogDb);
    assertEquals(DEFAULT_CATALOG_NAME, hiveDatabase.getCatalogName());
  }

  @Test
  public void testTableCatalogName() {
    Table catalogTable = TestObjects.getTestTable();
    org.apache.hadoop.hive.metastore.api.Table hiveTable = catalogToHiveConverter.convertTable(catalogTable, TEST_DB_NAME);
    assertEquals(DEFAULT_CATALOG_NAME, hiveTable.getCatName());
  }

  @Test
  public void testTableMetaCatalogName() {
    Table catalogTable = TestObjects.getTestTable();
    TableMeta tableMeta = catalogToHiveConverter.convertTableMeta(catalogTable, TEST_DB_NAME);
    assertEquals(DEFAULT_CATALOG_NAME, tableMeta.getCatName());
  }

  @Test
  public void testPartitionConversion() {
    Partition partition = TestObjects.getTestPartition(TEST_DB_NAME, TEST_TBL_NAME, ImmutableList.of("1"));
    org.apache.hadoop.hive.metastore.api.Partition hivePartition = catalogToHiveConverter.convertPartition(partition);
    assertEquals(DEFAULT_CATALOG_NAME, hivePartition.getCatName());
  }

  @Test
  public void testFunctionConversion() {
    UserDefinedFunction catalogFunction = TestObjects.getCatalogTestFunction();
    org.apache.hadoop.hive.metastore.api.Function hiveFunction = catalogToHiveConverter.convertFunction(TEST_DB_NAME, catalogFunction);
    assertEquals(DEFAULT_CATALOG_NAME, hiveFunction.getCatName());
  }
}
