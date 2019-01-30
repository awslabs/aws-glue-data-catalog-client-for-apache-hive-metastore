package com.amazonaws.glue.catalog.util;

import com.amazonaws.glue.catalog.converters.CatalogToHiveConverter;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static com.amazonaws.glue.catalog.util.TestObjects.getTestDatabase;
import static com.amazonaws.glue.catalog.util.TestObjects.getTestTable;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetastoreClientUtilsTest {

  private Warehouse wh;
  private HiveConf conf;

  private Path testPath;
  private static final String TEST_LOCATION = "s3://mybucket/";
  private Database testDb;
  private Table testTbl;

  @Before
  public void setup() {
    wh = mock(Warehouse.class);
    conf = new HiveConf();
    testPath = new Path(TEST_LOCATION);
    testDb = CatalogToHiveConverter.convertDatabase(getTestDatabase());
    testTbl = CatalogToHiveConverter.convertTable(getTestTable(), testDb.getName());
  }

  @Test(expected = NullPointerException.class)
  public void testMakeDirsNullWh() throws Exception {
    MetastoreClientUtils.makeDirs(null, testPath);
  }

  @Test(expected = NullPointerException.class)
  public void testMakeDirsNullPath() throws Exception {
    MetastoreClientUtils.makeDirs(wh, null);
  }

  @Test
  public void testMakeDirsAlreadyExists() throws Exception {
    when(wh.isDir(testPath)).thenReturn(true);
    assertFalse(MetastoreClientUtils.makeDirs(wh, testPath));
  }

  @Test(expected = MetaException.class)
  public void testMakeDirsCannotCreateDir() throws Exception {
    when(wh.isDir(testPath)).thenReturn(false);
    when(wh.mkdirs(testPath, true)).thenReturn(false);
    MetastoreClientUtils.makeDirs(wh, testPath);
  }

  @Test(expected = InvalidObjectException.class)
  public void testValidateTableObjectInvalidName() throws Exception {
    testTbl.setTableName("!");
    MetastoreClientUtils.validateTableObject(testTbl, conf);
  }

  @Test(expected = InvalidObjectException.class)
  public void testValidateTableObjectInvalidColumnName() throws Exception {
    testTbl.getSd().getCols().get(0).setType("invalidtype");
    MetastoreClientUtils.validateTableObject(testTbl, conf);
  }

  @Test(expected = InvalidObjectException.class)
  public void testValidateTableObjectInvalidPartitionKeys() throws Exception {
    testTbl.getPartitionKeys().get(0).setType("invalidtype");
    MetastoreClientUtils.validateTableObject(testTbl, conf);
  }

  @Test
  public void testDeepCopy() throws Exception {
    Map<String, String> orig = ImmutableMap.of("key", "val");
    Map<String, String> deepCopy = MetastoreClientUtils.deepCopyMap(orig);
    assertNotSame(deepCopy, orig);
    assertEquals(deepCopy, orig);
  }

  @Test
  public void testIsExternalTableFalse() {
    assertFalse(MetastoreClientUtils.isExternalTable(testTbl));
  }

  @Test
  public void testIsExternalTableParamTrue() {
    testTbl.getParameters().put("EXTERNAL", "true");
    assertTrue(MetastoreClientUtils.isExternalTable(testTbl));
  }

  @Test
  public void testIsExternalTableTableTypeTrue() {
    testTbl.setTableType(EXTERNAL_TABLE.name());
    testTbl.setParameters(null);
    assertTrue(MetastoreClientUtils.isExternalTable(testTbl));
  }

  @Test
  public void testIsExternalTableParamPriority() {
    // parameters has higher priority when there is conflict
    testTbl.getParameters().put("EXTERNAL", "false");
    testTbl.setTableType(EXTERNAL_TABLE.name());
    assertFalse(MetastoreClientUtils.isExternalTable(testTbl));
  }

  @Test
  public void testIsExternalTableNull() {
    assertFalse(MetastoreClientUtils.isExternalTable(null));
  }
}
