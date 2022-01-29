package com.amazonaws.glue.catalog.metastore.integrationtest;

import com.amazonaws.glue.catalog.converters.BaseCatalogToHiveConverter;
import com.amazonaws.glue.catalog.converters.CatalogToHiveConverter;
import com.amazonaws.glue.catalog.converters.GlueInputConverter;
import com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient;
import com.amazonaws.glue.catalog.metastore.GlueClientFactory;
import com.amazonaws.glue.catalog.util.GlueTestClientFactory;
import com.amazonaws.glue.catalog.util.TestObjects;
import com.amazonaws.glue.catalog.util.ExprBuilder;
import com.amazonaws.glue.shims.AwsGlueHiveShims;
import com.amazonaws.glue.shims.ShimsLoader;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.BatchCreatePartitionRequest;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.Table;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static com.amazonaws.glue.catalog.util.TestObjects.getTestDatabase;
import static com.amazonaws.glue.catalog.util.TestObjects.getTestTable;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetastoreClientPartitionIntegrationTest {

  private static AWSGlue glueClient;
  private static IMetaStoreClient metastoreClient;
  private static Database catalogDatabase;
  private static Table catalogTable;
  private AwsGlueHiveShims hiveShims = ShimsLoader.getHiveShims();
  private CatalogToHiveConverter catalogToHiveConverter = new BaseCatalogToHiveConverter();


  @BeforeClass
  public static void setUpForClass() throws MetaException {
    HiveConf conf = new HiveConf();
    Warehouse wh = mock(Warehouse.class);

    glueClient = new GlueTestClientFactory().newClient();
    GlueClientFactory clientFactory = mock(GlueClientFactory.class);
    when(clientFactory.newClient()).thenReturn(glueClient);

    metastoreClient = new AWSCatalogMetastoreClient.Builder().withHiveConf(conf).withWarehouse(wh)
        .withClientFactory(clientFactory).build();
    catalogDatabase = getTestDatabase();
    glueClient.createDatabase(new CreateDatabaseRequest().withDatabaseInput(
        GlueInputConverter.convertToDatabaseInput(catalogDatabase)));
    catalogTable = getTestTable();
  }

  @Before
  public void setup() throws TException {
    glueClient.createTable(new CreateTableRequest()
        .withDatabaseName(catalogDatabase.getName())
        .withTableInput(GlueInputConverter.convertToTableInput(catalogTable)));
  }

  @After
  public void cleanup() {
    glueClient.deleteTable(new DeleteTableRequest()
        .withDatabaseName(catalogDatabase.getName())
        .withName(catalogTable.getName()));
  }

  @AfterClass
  public static void cleanupAfterClass() {
    glueClient.deleteDatabase(new DeleteDatabaseRequest().withName(catalogDatabase.getName()));
  }

  @Test
  public void testCreatePartitions() throws Exception {
    String namespaceName = catalogDatabase.getName();
    String tableName = catalogTable.getName();
    Partition partition1 = TestObjects.getTestPartition(
        namespaceName, tableName, Lists.newArrayList("val1"));
    Partition partition2 = TestObjects.getTestPartition(
        namespaceName, tableName, Lists.newArrayList("val2"));
    List<Partition> partitions = Lists.newArrayList(partition1, partition2);
    List<org.apache.hadoop.hive.metastore.api.Partition> hivePartitions = Lists.newArrayList();
    for (Partition p : partitions) {
      hivePartitions.add(catalogToHiveConverter.convertPartition(p));
    }

    List<org.apache.hadoop.hive.metastore.api.Partition> partitionsCreated =
        metastoreClient.add_partitions(hivePartitions, false, true);
    assertEquals(2, partitionsCreated.size());
    for (org.apache.hadoop.hive.metastore.api.Partition hivePartition : hivePartitions) {
      assertThat(partitionsCreated, hasItem(hivePartition));
    }

    List<org.apache.hadoop.hive.metastore.api.Partition> partitionsCreated2 =
        metastoreClient.add_partitions(hivePartitions, true, true);
    assertEquals(0, partitionsCreated2.size());

    assertNull(metastoreClient.add_partitions(hivePartitions, true, false));
  }

  @Test
  public void testDropPartitions() throws Exception {
    String namespaceName = catalogDatabase.getName();
    String tableName = catalogTable.getName();
    Partition partition1 = TestObjects.getTestPartition(
        namespaceName, tableName, Lists.newArrayList("val1"));
    Partition partition2 = TestObjects.getTestPartition(
        namespaceName, tableName, Lists.newArrayList("val2"));
    List<Partition> partitions = Lists.newArrayList(partition1, partition2);
    glueClient.batchCreatePartition(new BatchCreatePartitionRequest()
      .withDatabaseName(namespaceName)
      .withTableName(tableName)
      .withPartitionInputList(GlueInputConverter.convertToPartitionInputs(partitions)));

     /*
      * We do nothing with boolean arguments ifExists and ignoreProtection.
      * If deleteData is true and the table is not external, hive will delete the actual table data.
      * It is beyond the concern of the scope of this test, so I set it to false.
      */
    List<org.apache.hadoop.hive.metastore.api.Partition> deletedPartitions = metastoreClient.dropPartitions(
        namespaceName, tableName, getDropPartitionExpressions(partitions), false, false, false);

    for (Partition p : partitions) {
      org.apache.hadoop.hive.metastore.api.Partition hivePartition = catalogToHiveConverter.convertPartition(p);
      assertTrue(containsPartitionIgnoreCreateTime(deletedPartitions, hivePartition));
    }
  }

  private List<ObjectPair<Integer, byte[]>> getDropPartitionExpressions(List<Partition> partitions) throws Exception {
    List<ObjectPair<Integer, byte[]>> partExprs = Lists.newArrayList();

    for (int i = 0; i < partitions.size(); i++) {
      Partition partition = partitions.get(i);
      // the catalogTable has only one partition key
      String partitionKey = catalogTable.getPartitionKeys().get(0).getName();
      String partitionValue = partition.getValues().get(0);
      ExprNodeGenericFuncDesc expr = new ExprBuilder(catalogTable.getName())
          .val(partitionValue).strCol(partitionKey).pred("=", 2).build();
      byte[] payload = hiveShims.getSerializeExpression(expr);
      partExprs.add(new ObjectPair<>(i, payload));
    }

    return partExprs;
  }

  private boolean containsPartitionIgnoreCreateTime(
      List<org.apache.hadoop.hive.metastore.api.Partition> partitionList,
      org.apache.hadoop.hive.metastore.api.Partition expectedPart) {
    for (org.apache.hadoop.hive.metastore.api.Partition partition : partitionList) {
      if (expectedPart.getDbName().equals(partition.getDbName()) &&
          expectedPart.getTableName().equals(partition.getTableName()) &&
          expectedPart.getValues().equals(partition.getValues())) {
        return true;
      }
    }
    return false;
  }

}
