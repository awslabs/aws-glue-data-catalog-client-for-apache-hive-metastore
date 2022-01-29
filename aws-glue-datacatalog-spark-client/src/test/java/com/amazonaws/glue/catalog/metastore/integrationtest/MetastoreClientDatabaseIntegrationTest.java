package com.amazonaws.glue.catalog.metastore.integrationtest;

import com.amazonaws.glue.catalog.converters.BaseCatalogToHiveConverter;
import com.amazonaws.glue.catalog.converters.CatalogToHiveConverter;
import com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient;
import com.amazonaws.glue.catalog.metastore.GlueClientFactory;
import com.amazonaws.glue.catalog.util.GlueTestClientFactory;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.EntityNotFoundException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.amazonaws.glue.catalog.util.TestObjects.getTestDatabase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetastoreClientDatabaseIntegrationTest {

  private AWSGlue glueClient;
  private IMetaStoreClient metastoreClient;
  private Warehouse wh;
  private Database hiveDB;
  private com.amazonaws.services.glue.model.Database catalogDB;
  private HiveConf conf;
  private Path tmpPath;
  private List<String> additionalDbForCleanup;
  private CatalogToHiveConverter catalogToHiveConverter = new BaseCatalogToHiveConverter();

  @Before
  public void setup() throws MetaException {
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
    hiveDB = catalogToHiveConverter.convertDatabase(catalogDB);

    additionalDbForCleanup = Lists.newArrayList();
  }

  @After
  public void clean() {
    try {
      glueClient.deleteDatabase(new DeleteDatabaseRequest().withName(hiveDB.getName()));
      
      for (String db : additionalDbForCleanup) {
        glueClient.deleteDatabase(new DeleteDatabaseRequest().withName(db));
      }
    } catch (EntityNotFoundException e) {
      //there will be no database to drop after drop database test, so swallow the exception
    }
  }

  @Test
  public void testDefaultDatabase() throws TException {
    // default db should exist
    metastoreClient.getDatabase("default");
  }

  @Test
  public void createValidDatabase() throws TException {
    metastoreClient.createDatabase(hiveDB);
    Database db = metastoreClient.getDatabase(hiveDB.getName());
    assertEquals(hiveDB, db);
  }

  @Test(expected = AlreadyExistsException.class)
  public void createDuplicateDatabase() throws TException {
    metastoreClient.createDatabase(hiveDB);
    metastoreClient.createDatabase(hiveDB);
  }

  @Test
  public void testDropValidDatabase() throws TException {
    metastoreClient.createDatabase(hiveDB);
    metastoreClient.dropDatabase(hiveDB.getName());
    assertDrop(hiveDB.getName());
  }

  @Test(expected = NoSuchObjectException.class)
  public void dropInvalidDatabase() throws TException {
    metastoreClient.dropDatabase(hiveDB.getName());
  }

  @Test
  public void dropInvalidDataBaseWithIgnoreUnknownDatabase() throws TException {
    metastoreClient.dropDatabase("unknown_db", false, true);
  }

  @Test
  public void listValidDatabases() throws TException {
    Database database2 = catalogToHiveConverter.convertDatabase(getTestDatabase());
    additionalDbForCleanup.add(database2.getName());
    metastoreClient.createDatabase(hiveDB);
    metastoreClient.createDatabase(database2);
    List<String> databaseName = metastoreClient.getAllDatabases();
    assertTrue(databaseName.contains(hiveDB.getName()));
    assertTrue(databaseName.contains(database2.getName()));
  }

  @Test(expected = NoSuchObjectException.class)
  public void getInvalidDatabase() throws TException {
    metastoreClient.getDatabase(hiveDB.getName());
  }

  @Test
  public void alterDatabase() throws TException {
    Map<String, String> parameters = Maps.newHashMap();
    parameters.put("param3", "value3");
    parameters.put("param4", "value4");

    metastoreClient.createDatabase(hiveDB);

    Database updatedDB = catalogToHiveConverter.convertDatabase(getTestDatabase());
    updatedDB.setName(hiveDB.getName());
    updatedDB.setParameters(parameters);

    metastoreClient.alterDatabase(hiveDB.getName(), updatedDB);
    Database afterUpdate = metastoreClient.getDatabase(hiveDB.getName());

    assertTrue(afterUpdate.getParameters().containsKey("param3"));
    assertTrue(afterUpdate.getParameters().containsKey("param4"));
  }

  private void assertDrop(final String databaseName) throws TException {
    boolean dropped = false;
    try {
      metastoreClient.getDatabase(databaseName);
    } catch (NoSuchObjectException e) {
      dropped = true;
    }
    assertTrue("Unable to drop database", dropped);
  }

}
