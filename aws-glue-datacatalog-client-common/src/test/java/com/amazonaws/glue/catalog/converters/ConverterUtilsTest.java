package com.amazonaws.glue.catalog.converters;

import com.amazonaws.glue.catalog.util.TestObjects;
import software.amazon.awssdk.services.glue.model.Table;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ConverterUtilsTest {

  @Test
  public void testCoralTableToStringConversion() {
    Table table = TestObjects.getTestTable();
    assertEquals(table, ConverterUtils.stringToCatalogTable(ConverterUtils.catalogTableToString(table)));
  }

}
