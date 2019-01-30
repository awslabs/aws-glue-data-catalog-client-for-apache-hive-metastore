package com.amazonaws.glue.catalog.util;

import com.amazonaws.services.glue.model.InvalidInputException;
import com.amazonaws.services.glue.model.Table;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.apache.hadoop.hive.metastore.TableType;

import static com.amazonaws.glue.catalog.util.HiveTableValidator.REQUIRED_PROPERTIES_VALIDATOR;
import static com.amazonaws.glue.catalog.util.TestObjects.getTestTable;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;

public class HiveTableValidatorTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();
  private static final String EXPECTED_MESSAGE = "%s cannot be null";

  @Test
  public void testRequiredProperty_TableType() {
    thrown.expect(InvalidInputException.class);
    thrown.expectMessage(String.format(EXPECTED_MESSAGE, "TableType"));
    Table tbl = getTestTable().withTableType(null);
    REQUIRED_PROPERTIES_VALIDATOR.validate(tbl);
  }

  @Test
  public void testRequiredProperty_StorageDescriptor() {
    thrown.expect(InvalidInputException.class);
    thrown.expectMessage(String.format(EXPECTED_MESSAGE, "StorageDescriptor"));
    Table tbl = getTestTable().withStorageDescriptor(null);
    REQUIRED_PROPERTIES_VALIDATOR.validate(tbl);
  }

  @Test
  public void testRequiredProperty_InputFormat() {
    thrown.expect(InvalidInputException.class);
    thrown.expectMessage(String.format(EXPECTED_MESSAGE, "StorageDescriptor#InputFormat"));
    Table tbl = getTestTable();
    tbl.getStorageDescriptor().setInputFormat(null);
    REQUIRED_PROPERTIES_VALIDATOR.validate(tbl);
  }

  @Test
  public void testRequiredProperty_OutputFormat() {
    thrown.expect(InvalidInputException.class);
    thrown.expectMessage(String.format(EXPECTED_MESSAGE, "StorageDescriptor#OutputFormat"));
    Table tbl = getTestTable();
    tbl.getStorageDescriptor().setOutputFormat(null);
    REQUIRED_PROPERTIES_VALIDATOR.validate(tbl);
  }

  @Test
  public void testRequiredProperty_SerdeInfo() {
    thrown.expect(InvalidInputException.class);
    thrown.expectMessage(String.format(EXPECTED_MESSAGE, "StorageDescriptor#SerdeInfo"));
    Table tbl = getTestTable();
    tbl.getStorageDescriptor().setSerdeInfo(null);
    REQUIRED_PROPERTIES_VALIDATOR.validate(tbl);
  }

  @Test
  public void testRequiredProperty_SerializationLibrary() {
    thrown.expect(InvalidInputException.class);
    thrown.expectMessage(String.format(EXPECTED_MESSAGE, "StorageDescriptor#SerdeInfo#SerializationLibrary"));
    Table tbl = getTestTable();
    tbl.getStorageDescriptor().getSerdeInfo().setSerializationLibrary(null);
    REQUIRED_PROPERTIES_VALIDATOR.validate(tbl);
  }

  @Test
  public void testRequiredProperty_ValidTable() {
    REQUIRED_PROPERTIES_VALIDATOR.validate(getTestTable());
  }

  @Test
  public void testValidate_ViewTableType() {
    Table tbl = getTestTable();
    tbl.setTableType(TableType.VIRTUAL_VIEW.name());
    tbl.getStorageDescriptor().getSerdeInfo().setSerializationLibrary(null);
    REQUIRED_PROPERTIES_VALIDATOR.validate(tbl);
  }

  @Test
  public void testValidate_ExcludeStorageHandlerType() {
    Table tbl = getTestTable();
    tbl.getParameters().put(META_TABLE_STORAGE, "org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler");
    tbl.getStorageDescriptor().setInputFormat(null);
    REQUIRED_PROPERTIES_VALIDATOR.validate(tbl);
  }
}
