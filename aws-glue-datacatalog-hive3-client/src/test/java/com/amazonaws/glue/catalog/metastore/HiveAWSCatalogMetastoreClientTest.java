package com.amazonaws.glue.catalog.metastore;

import com.amazonaws.glue.catalog.converters.CatalogToHiveConverter;
import com.amazonaws.glue.catalog.converters.Hive3CatalogToHiveConverter;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.UserDefinedFunction;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.Before;
import org.junit.Test;

import static com.amazonaws.glue.catalog.util.TestObjects.getCatalogTestFunction;
import static com.amazonaws.glue.catalog.util.TestObjects.getTestDatabase;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class HiveAWSCatalogMetastoreClientTest {

  private AWSGlue glueClient;
  private AWSCatalogMetastoreClient metastoreClient;
  private Configuration conf;
  private GlueClientFactory clientFactory;
  private CatalogToHiveConverter catalogToHiveConverter = new Hive3CatalogToHiveConverter();

  private org.apache.hadoop.hive.metastore.api.Database testDB;
  private org.apache.hadoop.hive.metastore.api.Function testFunction;
  private String catalogId;
  private UserDefinedFunction catalogTestFunction;

  @Before
  public void setUp() throws Exception {
    testDB = catalogToHiveConverter.convertDatabase(getTestDatabase());
    catalogTestFunction = getCatalogTestFunction();
    catalogTestFunction.setDatabaseName(testDB.getName());
    testFunction = catalogToHiveConverter.convertFunction(testDB.getName(), catalogTestFunction);
    // catalogId is a AWS account number
    catalogId = RandomStringUtils.randomNumeric(12);

    conf = spy(MetastoreConf.newMetastoreConf());
    glueClient = spy(AWSGlue.class);
    clientFactory = mock(GlueClientFactory.class);
    when(clientFactory.newClient()).thenReturn(glueClient);
    metastoreClient = new AWSCatalogMetastoreClient.Builder().withClientFactory(clientFactory)
        .createDefaults(false).withConf(conf).withCatalogId(catalogId).build();
  }

  @Test
  public void testPrimaryKeys_NotNull() throws Exception {
    PrimaryKeysRequest request = mock(PrimaryKeysRequest.class);
    assertNotNull(metastoreClient.getPrimaryKeys(request));
  }

  @Test
  public void testForeignKeys_NotNull() throws Exception {
    ForeignKeysRequest request = mock(ForeignKeysRequest.class);
    assertNotNull(metastoreClient.getForeignKeys(request));
  }

  @Test
  public void testGetNextNotification() throws Exception {
    // check that we just return dummy result
    assertNotNull(metastoreClient.getNextNotification(0, 1, null));
  }

  @Test
  public void testGetCurrentNotificationEventId() throws Exception {
    // check that we get dummy result with 0 eventId
    assertEquals(0, metastoreClient.getCurrentNotificationEventId().getEventId());
  }
}
