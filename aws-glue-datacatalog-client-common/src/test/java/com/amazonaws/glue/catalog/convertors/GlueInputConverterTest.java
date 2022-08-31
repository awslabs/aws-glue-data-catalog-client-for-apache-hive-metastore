package com.amazonaws.glue.catalog.convertors;

import com.amazonaws.glue.catalog.converters.CatalogToHiveConverter;
import com.amazonaws.glue.catalog.converters.GlueInputConverter;
import com.amazonaws.glue.catalog.util.TestObjects;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UserDefinedFunction;
import com.amazonaws.services.glue.model.UserDefinedFunctionInput;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class GlueInputConverterTest {

  private Database testDB;
  private Table testTable;
  private Partition testPartition;
  private UserDefinedFunction testFunction;

  @Before
  public void setup() {
    testDB = TestObjects.getTestDatabase();
    testTable = TestObjects.getTestTable();
    testPartition = TestObjects.getTestPartition(testDB.getName(), testTable.getName(), Lists.newArrayList("val1"));
    testFunction = TestObjects.getCatalogTestFunction();
  }

  @Test
  public void testConvertHiveDbToDatabaseInput() {
    org.apache.hadoop.hive.metastore.api.Database hivedb = CatalogToHiveConverter.convertDatabase(testDB);
    DatabaseInput dbInput = GlueInputConverter.convertToDatabaseInput(hivedb);

    assertEquals(testDB.getName(), dbInput.getName());
    assertEquals(testDB.getDescription(), dbInput.getDescription());
    assertEquals(testDB.getLocationUri(), dbInput.getLocationUri());
    assertEquals(testDB.getParameters(), dbInput.getParameters());
  }

  @Test
  public void testConvertCatalogDbToDatabaseInput() {
    DatabaseInput dbInput = GlueInputConverter.convertToDatabaseInput(testDB);

    assertEquals(testDB.getName(), dbInput.getName());
    assertEquals(testDB.getDescription(), dbInput.getDescription());
    assertEquals(testDB.getLocationUri(), dbInput.getLocationUri());
    assertEquals(testDB.getParameters(), dbInput.getParameters());
  }

  @Test
  public void testConvertHiveTableToTableInput() {
    org.apache.hadoop.hive.metastore.api.Table hivetbl = CatalogToHiveConverter.convertTable(testTable, testDB.getName());
    TableInput tblInput = GlueInputConverter.convertToTableInput(hivetbl);

    assertEquals(testTable.getName(), tblInput.getName());
    assertEquals(testTable.getOwner(), tblInput.getOwner());
    assertEquals(testTable.getTableType(), tblInput.getTableType());
    assertEquals(testTable.getParameters(), tblInput.getParameters());
    assertEquals(testTable.getPartitionKeys(), tblInput.getPartitionKeys());
    assertEquals(testTable.getRetention(), tblInput.getRetention());
    assertEquals(testTable.getLastAccessTime(), tblInput.getLastAccessTime());
    assertEquals(testTable.getStorageDescriptor(), tblInput.getStorageDescriptor());
    assertEquals(testTable.getViewExpandedText(), tblInput.getViewExpandedText());
    assertEquals(testTable.getViewOriginalText(), tblInput.getViewOriginalText());
  }

  @Test
  public void testConvertCatalogTableToTableInput() {
    TableInput tblInput = GlueInputConverter.convertToTableInput(testTable);

    assertEquals(testTable.getName(), tblInput.getName());
    assertEquals(testTable.getOwner(), tblInput.getOwner());
    assertEquals(testTable.getTableType(), tblInput.getTableType());
    assertEquals(testTable.getParameters(), tblInput.getParameters());
    assertEquals(testTable.getPartitionKeys(), tblInput.getPartitionKeys());
    assertEquals(testTable.getRetention(), tblInput.getRetention());
    assertEquals(testTable.getLastAccessTime(), tblInput.getLastAccessTime());
    assertEquals(testTable.getStorageDescriptor(), tblInput.getStorageDescriptor());
    assertEquals(testTable.getViewExpandedText(), tblInput.getViewExpandedText());
    assertEquals(testTable.getViewOriginalText(), tblInput.getViewOriginalText());
  }

  @Test
  public void testConvertHivePartitionToPartitionInput() {
    org.apache.hadoop.hive.metastore.api.Partition hivePartition = CatalogToHiveConverter.convertPartition(testPartition);
    PartitionInput partitionInput = GlueInputConverter.convertToPartitionInput(hivePartition);

    assertEquals(testPartition.getLastAccessTime(), partitionInput.getLastAccessTime());
    assertEquals(testPartition.getParameters(), partitionInput.getParameters());
    assertEquals(testPartition.getStorageDescriptor(), partitionInput.getStorageDescriptor());
    assertEquals(testPartition.getValues(), partitionInput.getValues());
  }

  @Test
  public void testConvertCatalogPartitionToPartitionInput() {
    PartitionInput partitionInput = GlueInputConverter.convertToPartitionInput(testPartition);

    assertEquals(testPartition.getLastAccessTime(), partitionInput.getLastAccessTime());
    assertEquals(testPartition.getParameters(), partitionInput.getParameters());
    assertEquals(testPartition.getStorageDescriptor(), partitionInput.getStorageDescriptor());
    assertEquals(testPartition.getValues(), partitionInput.getValues());
  }

  @Test
  public void testConvertHiveFunctionToFunctionInput() {
    org.apache.hadoop.hive.metastore.api.Function hiveFunction =
        CatalogToHiveConverter.convertFunction(testDB.getName(), testFunction);
    UserDefinedFunctionInput functionInput = GlueInputConverter.convertToUserDefinedFunctionInput(hiveFunction);

    assertEquals(testFunction.getClassName(), functionInput.getClassName());
    assertEquals(testFunction.getFunctionName(), functionInput.getFunctionName());
    assertEquals(testFunction.getOwnerName(), functionInput.getOwnerName());
    assertEquals(testFunction.getOwnerType(), functionInput.getOwnerType());
    assertEquals(testFunction.getResourceUris(), functionInput.getResourceUris());
  }

  @Test
  public void testConvertHiveFunctionToFunctionInputNullOwnerType() {
    org.apache.hadoop.hive.metastore.api.Function hiveFunction =
      CatalogToHiveConverter.convertFunction(testDB.getName(), testFunction);
    hiveFunction.setOwnerType(null);
    GlueInputConverter.convertToUserDefinedFunctionInput(hiveFunction);
  }

}
