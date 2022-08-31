package com.amazonaws.glue.catalog.metastore.integrationtest;

import com.amazonaws.glue.catalog.converters.CatalogToHiveConverter;
import com.amazonaws.glue.catalog.converters.HiveToCatalogConverter;
import com.amazonaws.glue.catalog.util.ExprBuilder;
import com.amazonaws.glue.catalog.util.TestObjects;
import com.amazonaws.glue.shims.AwsGlueHiveShims;
import com.amazonaws.glue.shims.ShimsLoader;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.GetPartitionRequest;
import com.amazonaws.services.glue.model.GetPartitionsRequest;
import com.amazonaws.services.glue.model.Partition;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.amazonaws.glue.catalog.util.TestObjects.getTestDatabase;
import static com.amazonaws.glue.catalog.util.TestObjects.getTestTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MultipleCatalogPartitionIntegrationTest extends MultipleCatalogIntegrationTestBase {
    private Database databaseInAnotherCatalog;
    private Database database;
    private Table table;
    private Partition partition1;
    private Partition partition2;
    private Partition partition3;
    private AwsGlueHiveShims hiveShims = ShimsLoader.getHiveShims();

    @Before
    public void setup() throws MetaException, TException {
        super.setup();

        databaseInAnotherCatalog = CatalogToHiveConverter.convertDatabase(getTestDatabase());
        database = databaseInAnotherCatalog.deepCopy();
        database.setName(constructDbName(databaseInAnotherCatalog.getName()));
        metastoreClient.createDatabase(database);

        table = CatalogToHiveConverter.convertTable(getTestTable(), database.getName());
        metastoreClient.createTable(table);

        partition1 = TestObjects.getTestPartition(
                database.getName(), table.getTableName(), Lists.newArrayList("val1"));
        partition2 = TestObjects.getTestPartition(
                database.getName(), table.getTableName(), Lists.newArrayList("val2"));
        partition3 = TestObjects.getTestPartition(
                database.getName(), table.getTableName(), Lists.newArrayList("val3"));

        metastoreClient.add_partitions(Lists.newArrayList(
                CatalogToHiveConverter.convertPartition(partition1),
                CatalogToHiveConverter.convertPartition(partition2),
                CatalogToHiveConverter.convertPartition(partition3)
        ));
    }

    @After
    public void clean() {
        glueClient.deleteDatabase(new DeleteDatabaseRequest().withName(database.getName()));
    }

    private boolean containsPartition(List<Partition> partitions, String val) {
        for (Partition partition: partitions) {
            if (partition.getValues().size() == 1
                && partition.getValues().get(0).equals(val)) {
                return true;
            }
        }
        return false;
    }

    @Test
    public void getPartition() throws TException {
        org.apache.hadoop.hive.metastore.api.Partition partition = metastoreClient.getPartition(
                database.getName(), table.getTableName(), ImmutableList.of("val1"));
        assertEquals(database.getName(), partition.getDbName());
        assertEquals(table.getTableName(), partition.getTableName());
        assertEquals(1, partition.getValues().size());
        assertEquals("val1", partition.getValues().get(0));
    }

    @Test
    public void listPartitions() throws TException {
        String partitionColName = table.getPartitionKeys().get(0).getName();
        String expression = String.format("(%s = 'val1') or (%s = 'val2') or (%s = 'val3')",
                partitionColName, partitionColName, partitionColName);
        List<Partition> partitions = metastoreClient.listPartitionsByFilter(
                database.getName(),
                table.getTableName(),
                expression,
                (short)10)
                .stream()
                .map(HiveToCatalogConverter::convertPartition)
                .collect(Collectors.toList());
        assertEquals(3, partitions.size());
        assertTrue(containsPartition(partitions, "val1"));
        assertTrue(containsPartition(partitions, "val2"));
        assertTrue(containsPartition(partitions, "val3"));

        partitions = glueClient.getPartitions(new GetPartitionsRequest()
                .withCatalogId(anotherCatalogId)
                .withDatabaseName(databaseInAnotherCatalog.getName())
                .withTableName(table.getTableName())
                .withExpression(expression)
        ).getPartitions();

        assertEquals(3, partitions.size());
        assertTrue(containsPartition(partitions, "val1"));
        assertTrue(containsPartition(partitions, "val2"));
        assertTrue(containsPartition(partitions, "val3"));
    }

    @Test
    public void alterPartition() throws TException {
        Map<String, String> newParameters = ImmutableMap.of("newKey", "newVal");
        partition1.setParameters(newParameters);
        metastoreClient.alter_partition(
                database.getName(),
                table.getTableName(),
                CatalogToHiveConverter.convertPartition(partition1)
        );

        Partition alteredPartition = glueClient.getPartition(new GetPartitionRequest()
                .withCatalogId(anotherCatalogId)
                .withDatabaseName(databaseInAnotherCatalog.getName())
                .withTableName(table.getTableName())
                .withPartitionValues("val1")
        ).getPartition();

        assertEquals("newVal", alteredPartition.getParameters().get("newKey"));
    }

    @Test
    public void dropPartition() throws TException {
        metastoreClient.dropPartition(
                database.getName(),
                table.getTableName(),
                ImmutableList.of("val1"),
                false
        );

        List<Partition> partitions = glueClient.getPartitions(new GetPartitionsRequest()
                .withCatalogId(anotherCatalogId)
                .withDatabaseName(databaseInAnotherCatalog.getName())
                .withTableName(table.getTableName())
        ).getPartitions();

        assertEquals(2, partitions.size());
        assertTrue(containsPartition(partitions, "val2"));
        assertTrue(containsPartition(partitions, "val3"));
    }

    @Test
    public void dropPartitions() throws Exception {
        String partitionColName = table.getPartitionKeys().get(0).getName();
        List<ObjectPair<Integer, byte[]>> partExprs = Lists.newArrayList();
        partExprs.add(new ObjectPair<>(0, hiveShims.getSerializeExpression(
                new ExprBuilder(table.getTableName())
                        .val("val1").strCol(partitionColName).pred("=", 2).build())));
        partExprs.add(new ObjectPair<>(0, hiveShims.getSerializeExpression(
                new ExprBuilder(table.getTableName())
                        .val("val2").strCol(partitionColName).pred("=", 2).build())));
        metastoreClient.dropPartitions(
                database.getName(),
                table.getTableName(),
                partExprs,
                false,
                false,
                false
        );

        List<Partition> partitions = glueClient.getPartitions(new GetPartitionsRequest()
                .withCatalogId(anotherCatalogId)
                .withDatabaseName(databaseInAnotherCatalog.getName())
                .withTableName(table.getTableName())
        ).getPartitions();

        assertEquals(1, partitions.size());
        assertTrue(containsPartition(partitions, "val3"));
    }
}
