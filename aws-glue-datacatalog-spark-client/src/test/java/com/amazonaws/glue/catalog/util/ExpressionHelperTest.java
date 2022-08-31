package com.amazonaws.glue.catalog.util;

import com.amazonaws.glue.shims.AwsGlueHiveShims;
import com.amazonaws.glue.shims.ShimsLoader;
import com.amazonaws.services.glue.model.Table;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

import static com.amazonaws.glue.catalog.util.TestObjects.getTestTable;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExpressionHelperTest {

  private org.apache.hadoop.hive.metastore.api.Table table;
  private static String testDate;
  private final static AwsGlueHiveShims hiveShims = ShimsLoader.getHiveShims();
  private final static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

  private org.apache.hadoop.hive.metastore.api.FieldSchema makeFieldSchema(String name, String type) {
    org.apache.hadoop.hive.metastore.api.FieldSchema schema =
        new org.apache.hadoop.hive.metastore.api.FieldSchema();
    schema.setName(name);
    schema.setType(type);
    return schema;
  }

  @BeforeClass
  static public void setUpOnce() {
    dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    testDate = dateFormat.format(new Date(123456L));
  }

  @Before
  public void setUp() {
    List<FieldSchema> partitionKeys = ImmutableList.of(
          makeFieldSchema("name", "string"),
          makeFieldSchema("birthday", "date"),
          makeFieldSchema("age", "int")
    );

    table = mock(org.apache.hadoop.hive.metastore.api.Table.class);
    when(table.getPartitionKeys()).thenReturn(partitionKeys);
  }

  @Test
  public void testExpressionConversionByTimestamp() throws Exception {
    Table table = getTestTable();
    ExprBuilder e = new ExprBuilder(table.getName()).val(testDate).timestampCol("timestamp").pred("=", 2);
    ExprNodeGenericFuncDesc exprTree = e.build();
    byte[] payload = hiveShims.getSerializeExpression(exprTree);
    String expression = ExpressionHelper.convertHiveExpressionToCatalogExpression(payload);
    assertEquals("(timestamp = '1970-01-01 00:02:03.456')", expression);
  }

  @Test
  public void testExpressionConversionWithStringAndTimestampColumn() throws Exception {
    Table table = getTestTable();
    ExprNodeGenericFuncDesc expr = new ExprBuilder(table.getName())
          .val(testDate).timestampCol("timestamp").pred("=", 2)
          .val("test").strCol("strCol").pred("=", 2)
          .pred("and", 2).build();
    byte[] payload = hiveShims.getSerializeExpression(expr);
    String filter = ExpressionHelper.convertHiveExpressionToCatalogExpression(payload);
    assertEquals("((strCol = 'test') and (timestamp = '1970-01-01 00:02:03.456'))", filter);
  }

  @Test
  public void testExpressionConversionWithNestedStringAndTimestampColumns() throws Exception {
    Table table = getTestTable();
    ExprNodeGenericFuncDesc expr = new ExprBuilder(table.getName())
          .val(testDate).timestampCol("timestamp").pred("=", 2)
          .val("test").strCol("strCol").pred("=", 2)
          .pred("and", 2)
          .val(testDate).timestampCol("timestamp").pred("=", 2)
          .val("test").strCol("strCol").pred("=", 2)
          .pred("and", 2)
          .pred("and", 2).build();
    byte[] payload = hiveShims.getSerializeExpression(expr);
    String filter = ExpressionHelper.convertHiveExpressionToCatalogExpression(payload);
    assertEquals("(((strCol = 'test') and (timestamp = '1970-01-01 00:02:03.456')) " +
                "and " +
                "((strCol = 'test') and (timestamp = '1970-01-01 00:02:03.456')))",
          filter);
  }

  @Test
  public void testExpressionConversionWithNotIn() throws Exception {
    Table table = getTestTable();
    ExprNodeGenericFuncDesc expr = new ExprBuilder(table.getName())
          .vals(Arrays.asList("val1", "val2", "val3")).strCol("strCol").pred("in", 4)
          .pred("not", 1)
          .build();
    byte[] payload = hiveShims.getSerializeExpression(expr);
    String filter = ExpressionHelper.convertHiveExpressionToCatalogExpression(payload);
    assertEquals("((strCol) NOT IN ('val3', 'val2', 'val1'))", filter);
  }

  @Test
  public void testExpressionConversionWithIn() throws Exception {
    Table table = getTestTable();
    ExprNodeGenericFuncDesc expr = new ExprBuilder(table.getName())
          .vals(Arrays.asList("val1", "val2", "val3")).strCol("strCol").pred("in", 4)
          .build();
    byte[] payload = hiveShims.getSerializeExpression(expr);
    String filter = ExpressionHelper.convertHiveExpressionToCatalogExpression(payload);
    assertEquals("(strCol) IN ('val3', 'val2', 'val1')", filter);
  }

  @Test
  public void testExpressionConversionWithBetween() throws Exception {
    Table table = getTestTable();
    ExprNodeGenericFuncDesc expr = new ExprBuilder(table.getName())
          .val("val100").val("val1").strCol("strCol").val(false).pred("between", 4)
          .build();
    byte[] payload = hiveShims.getSerializeExpression(expr);
    String filter = ExpressionHelper.convertHiveExpressionToCatalogExpression(payload);
    assertEquals("strCol BETWEEN 'val1' AND 'val100'", filter);
  }

  @Test
  public void testExpressionConversionWithNotBetween() throws Exception {
    Table table = getTestTable();
    ExprNodeGenericFuncDesc expr = new ExprBuilder(table.getName())
          .val("val100" ).val("val1").strCol("strCol").val(true).pred("between", 4)
          .build();
    byte[] payload = hiveShims.getSerializeExpression(expr);
    String filter = ExpressionHelper.convertHiveExpressionToCatalogExpression(payload);
    assertEquals("strCol NOT BETWEEN 'val1' AND 'val100'", filter);
  }

  @Test
  public void testExpressionConversionWithMultipleNots() throws Exception {
    Table table = getTestTable();
    ExprNodeGenericFuncDesc expr = new ExprBuilder(table.getName())
          .val("val100" ).val("val1").strCol("strCol").val(true).pred("between", 4)
          .vals(Arrays.asList("val1", "val2", "val3")).strCol("strCol").pred("in", 4)
          .pred("not", 1)
          .pred("and", 2)
          .build();
    byte[] payload = hiveShims.getSerializeExpression(expr);
    String filter = ExpressionHelper.convertHiveExpressionToCatalogExpression(payload);
    assertEquals("(((strCol) NOT IN ('val3', 'val2', 'val1')) and strCol NOT BETWEEN 'val1' AND 'val100')", filter);
  }

  @Test
  public void testExpressionConversionWithTwoNotInAndOneIn() throws Exception {
    Table table = getTestTable();
    ExprNodeGenericFuncDesc expr = new ExprBuilder(table.getName())
          .vals(Arrays.asList("data1", "data2")).strCol("notInCol1").pred("in", 3)
          .pred("not", 1)
          .vals(Arrays.asList("val1", "val2", "val3")).strCol("notInCol2").pred("in", 4)
          .pred("not", 1)
          .pred("and", 2)
          .vals(Arrays.asList("abc", "def", "ghi", "jkl")).strCol("inCol1").pred("in", 5)
          .pred("and", 2)
          .build();
    byte[] payload = hiveShims.getSerializeExpression(expr);
    String filter = ExpressionHelper.convertHiveExpressionToCatalogExpression(payload);
    assertEquals("((inCol1) IN ('jkl', 'ghi', 'def', 'abc') and (((notInCol2) NOT IN ('val3', 'val2', 'val1')) and " +
          "((notInCol1) NOT IN ('data2', 'data1'))))", filter);
  }

  @Test
  public void testExpressionConversionWithTwoNotInsSameColumn() throws Exception {
    Table table = getTestTable();
    ExprNodeGenericFuncDesc expr = new ExprBuilder(table.getName())
          .vals(Arrays.asList("data1", "data2")).strCol("strCol").pred("in", 3)
          .pred("not", 1)
          .vals(Arrays.asList("val1", "val2", "val3")).strCol("strCol").pred("in", 4)
          .pred("not", 1)
          .pred("or", 2)
          .build();
    byte[] payload = hiveShims.getSerializeExpression(expr);
    String filter = ExpressionHelper.convertHiveExpressionToCatalogExpression(payload);
    assertEquals("(((strCol) NOT IN ('val3', 'val2', 'val1')) or ((strCol) NOT IN ('data2', 'data1')))", filter);
  }

  @Test
  public void testBuildExpressionFromPartialSpecification() throws MetaException {
    List<String> partitionValues = Arrays.asList("foo", "2017-01-02", "99");
    String expression = ExpressionHelper.buildExpressionFromPartialSpecification(table, partitionValues);
    assertThat(expression, is("(name='foo') AND (birthday='2017-01-02') AND (age=99)"));

    partitionValues = Arrays.asList("foo", "2017-01-02");
    expression = ExpressionHelper.buildExpressionFromPartialSpecification(table, partitionValues);
    assertThat(expression, is("(name='foo') AND (birthday='2017-01-02')"));

    partitionValues = Arrays.asList("", "2017-01-02");
    expression = ExpressionHelper.buildExpressionFromPartialSpecification(table, partitionValues);
    assertThat(expression, is("(birthday='2017-01-02')"));

    partitionValues = Arrays.asList("foo", "", "99");
    expression = ExpressionHelper.buildExpressionFromPartialSpecification(table, partitionValues);
    assertThat(expression, is("(name='foo') AND (age=99)"));

    partitionValues = new LinkedList<>();
    expression = ExpressionHelper.buildExpressionFromPartialSpecification(table, partitionValues);
    assertThat(expression, nullValue());

    assertNull(ExpressionHelper.buildExpressionFromPartialSpecification(table, null));
  }

  @Test(expected = MetaException.class)
  public void testBuildExpressionWithTooManyValues() throws MetaException {
    List<String> partitionValues = Arrays.asList("foo", "2017-01-02", "99", "abcd");
    ExpressionHelper.buildExpressionFromPartialSpecification(table, partitionValues);
  }

  @Test(expected = MetaException.class)
  public void testBuildExpressionWithNullPartitionKeys() throws MetaException {
    when(table.getPartitionKeys()).thenReturn(null);
    List<String> partitionValues = Arrays.asList("foo", "2017-01-02", "99");
    ExpressionHelper.buildExpressionFromPartialSpecification(table, partitionValues);
  }

  @Test
  public void testBuildExpressionEscapeQuotes() throws MetaException {
    List<String> partitionValues = Arrays.asList("'hello'", "ab'cd", "ab'cd");
    String expression = ExpressionHelper.buildExpressionFromPartialSpecification(table, partitionValues);
    assertThat(expression, is("(name='\\'hello\\'') AND (birthday='ab\\'cd') AND (age=ab'cd)"));
  }

  @Test
  public void testExpressionConversionWithDateCharAndBooleanColumn() throws Exception {
    ExprNodeGenericFuncDesc expr = new ExprBuilder("fooTable")
            // see https://goo.gl/eGEUh2, Hive uses the same method to convert query string value to Date object
            .val(java.sql.Date.valueOf("2017-11-14")).dateCol("dateCol").pred("=", 2)
            .val('c').charCol("charCol").pred("=", 2).pred("and", 2)
            .val(true).booleanCol("booleanCol").pred("=", 2).pred("and", 2).build();
    byte[] payload = hiveShims.getSerializeExpression(expr);

    String catalogExpression = ExpressionHelper.convertHiveExpressionToCatalogExpression(payload);
    assertEquals("((booleanCol = true) and ((charCol = 'c') and (dateCol = '2017-11-14')))",
            catalogExpression);
  }
}
