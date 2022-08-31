package com.amazonaws.glue.catalog;

import com.amazonaws.glue.catalog.converters.CatalogToHiveConverter;
import com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient;
import com.amazonaws.glue.catalog.metastore.GlueClientFactory;
import com.amazonaws.glue.catalog.metastore.GlueMetastoreClientDelegate;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.GetDatabasesRequest;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionsRequest;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionsResult;
import com.amazonaws.services.glue.model.UserDefinedFunction;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static com.amazonaws.glue.catalog.util.TestObjects.getCatalogTestFunction;
import static com.amazonaws.glue.catalog.util.TestObjects.getTestDatabase;
import static com.amazonaws.glue.catalog.util.TestObjects.getTestTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HiveAWSCatalogMetastoreClientTest {

  private AWSGlue glueClient;
  private AWSCatalogMetastoreClient metastoreClient;
  private HiveConf conf;
  private GlueClientFactory clientFactory;

  private org.apache.hadoop.hive.metastore.api.Database testDB;
  private org.apache.hadoop.hive.metastore.api.Table testTable;
  private org.apache.hadoop.hive.metastore.api.Function testFunction;
  private org.apache.hadoop.hive.metastore.Warehouse wh;
  private String catalogId;
  private UserDefinedFunction catalogTestFunction;

  @Before
  public void setUp() throws Exception {
    testDB = CatalogToHiveConverter.convertDatabase(getTestDatabase());
    testTable = CatalogToHiveConverter.convertTable(getTestTable(testDB.getName()), testDB.getName());

    catalogTestFunction = getCatalogTestFunction();
    testFunction = CatalogToHiveConverter.convertFunction(testDB.getName(), catalogTestFunction);
    catalogTestFunction.setDatabaseName(testDB.getName());
    // catalogId is a AWS account number
    catalogId = RandomStringUtils.randomNumeric(12);

    conf = spy(new HiveConf());
    glueClient = spy(AWSGlue.class);
    clientFactory = mock(GlueClientFactory.class);
    when(clientFactory.newClient()).thenReturn(glueClient);
    metastoreClient = new AWSCatalogMetastoreClient.Builder().withClientFactory(clientFactory)
        .createDefaults(false).withHiveConf(conf).withCatalogId(catalogId).build();
  }

  @Test
  public void testGetAllFunctions() throws Exception {
    when(glueClient.getUserDefinedFunctions(any(GetUserDefinedFunctionsRequest.class)))
        .thenReturn(new GetUserDefinedFunctionsResult().withUserDefinedFunctions(catalogTestFunction)
            .withNextToken("nexttoken"))
        .thenReturn(new GetUserDefinedFunctionsResult().withUserDefinedFunctions(catalogTestFunction));

    GetAllFunctionsResponse getAllFunctionsResponse = metastoreClient.getAllFunctions();
    verify(glueClient, never()).getDatabases(any(GetDatabasesRequest.class));
    verify(glueClient, times(2)).getUserDefinedFunctions(any(GetUserDefinedFunctionsRequest.class));
    assertEquals(2, getAllFunctionsResponse.getFunctionsSize());
    assertEquals(testFunction, getAllFunctionsResponse.getFunctions().get(0));
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
  public void testAlterTableCascade() throws Exception {
    String databaseName = "database-name";
    String tableName = "table-name";
    wh = mock(org.apache.hadoop.hive.metastore.Warehouse.class);
    GlueMetastoreClientDelegate mockDelegate = mock(GlueMetastoreClientDelegate.class);
    AWSCatalogMetastoreClient metaClient = new AWSCatalogMetastoreClient.Builder().withClientFactory(clientFactory)
        .withWarehouse(wh).withGlueMetastoreClientDelegate(mockDelegate).createDefaults(false).withHiveConf(conf).build();
    metaClient = spy(metaClient);

    metaClient.alter_table(databaseName, tableName, testTable, true);

    ArgumentCaptor<EnvironmentContext> captor = ArgumentCaptor.forClass(EnvironmentContext.class);
    verify(mockDelegate).alterTable(any(), any(), any(), captor.capture());
    assertEquals(StatsSetupConst.TRUE, captor.getValue().getProperties().get("CASCADE"));
  }
}
