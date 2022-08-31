package com.amazonaws.glue.catalog.metastore;

import com.amazonaws.glue.catalog.util.TestObjects;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.UserDefinedFunction;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class SparkCatalogMetastoreClientTest {

  private AWSGlue glueClient;
  private AWSCatalogMetastoreClient metastoreClient;
  private Warehouse wh;
  private HiveConf conf;
  private GlueClientFactory clientFactory;

  // Test objects
  private Database testDB;
  private UserDefinedFunction testFunction;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() throws Exception{
    testDB = TestObjects.getTestDatabase();
    testFunction = TestObjects.getCatalogTestFunction();

    conf = spy(new HiveConf());
    glueClient = mock(AWSGlue.class);
    clientFactory = mock(GlueClientFactory.class);
    when(clientFactory.newClient()).thenReturn(glueClient);
    metastoreClient = new AWSCatalogMetastoreClient.Builder().withClientFactory(clientFactory)
      .withWarehouse(wh).createDefaults(false).withHiveConf(conf).build();
  }

  @Test
  public void testGetFunctionNoSuchObjectExceptionMessage() throws Exception {
    expectedException.expect(NoSuchObjectException.class);
    expectedException.expectMessage(testFunction.getFunctionName() + " does not exist");

    when(glueClient.getUserDefinedFunction(any(GetUserDefinedFunctionRequest.class)))
      .thenThrow(new EntityNotFoundException(""));
    metastoreClient.getFunction(testDB.getName(), testFunction.getFunctionName());
  }

}
