package com.amazonaws.glue.catalog.convertors;

import com.amazonaws.glue.catalog.converters.ConverterUtils;
import com.amazonaws.glue.catalog.util.TestObjects;
import com.amazonaws.services.glue.model.Table;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ConverterUtilsTest {

  @Test
  public void testCoralTableToStringConversion() {
    Table table = TestObjects.getTestTable();
    assertEquals(table, ConverterUtils.stringToCatalogTable(ConverterUtils.catalogTableToString(table)));
  }

}
