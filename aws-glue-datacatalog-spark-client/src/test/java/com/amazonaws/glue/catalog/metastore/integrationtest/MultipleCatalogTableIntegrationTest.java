package com.amazonaws.glue.catalog.metastore.integrationtest;

import com.amazonaws.glue.catalog.converters.BaseCatalogToHiveConverter;
import com.amazonaws.glue.catalog.converters.CatalogToHiveConverter;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.amazonaws.glue.catalog.util.TestObjects.getTestDatabase;
import static com.amazonaws.glue.catalog.util.TestObjects.getTestTable;
import static org.junit.Assert.assertEquals;

public class MultipleCatalogTableIntegrationTest extends MultipleCatalogIntegrationTestBase {
    private Database databaseInAnotherCatalog;
    private Database database;
    private Table table;
    private CatalogToHiveConverter catalogToHiveConverter = new BaseCatalogToHiveConverter();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() throws MetaException, TException {
        super.setup();

        databaseInAnotherCatalog = catalogToHiveConverter.convertDatabase(getTestDatabase());
        database = databaseInAnotherCatalog.deepCopy();
        database.setName(constructDbName(databaseInAnotherCatalog.getName()));
        metastoreClient.createDatabase(database);

        table = catalogToHiveConverter.convertTable(getTestTable(), database.getName());
        metastoreClient.createTable(table);
    }

    @After
    public void clean() {
        glueClient.deleteDatabase(new DeleteDatabaseRequest().withName(database.getName()));
    }

    @Test
    public void getTable() throws TException {
        Table createdTable = metastoreClient.getTable(database.getName(), table.getTableName());
        //time is updated on service side and can be different from what we sent
        table.setCreateTime(createdTable.getCreateTime());
        table.setLastAccessTime(createdTable.getLastAccessTime());
        assertEquals(table, createdTable);

        createdTable = catalogToHiveConverter.convertTable(
                glueClient.getTable(new GetTableRequest()
                        .withCatalogId(anotherCatalogId)
                        .withDatabaseName(databaseInAnotherCatalog.getName())
                        .withName(table.getTableName())
                ).getTable(), database.getName());
        assertEquals(table, createdTable);
    }

    @Test
    public void getAllTables() throws TException {
        Table table2 = catalogToHiveConverter.convertTable(getTestTable(), database.getName());
        metastoreClient.createTable(table2);

        Table table3 = catalogToHiveConverter.convertTable(getTestTable(), database.getName());
        metastoreClient.createTable(table3);

        Set<String> tableNames = new HashSet<>(metastoreClient.getAllTables(database.getName()));
        Set<String> expectedNames = ImmutableSet.of(table.getTableName(), table2.getTableName(), table3.getTableName());
        assertEquals(expectedNames, tableNames);
    }

    @Test
    public void alterTable() throws TException {
        Map<String, String> newParameters = ImmutableMap.of("param1", "newVal1");
        table.setParameters(newParameters);
        metastoreClient.alter_table(database.getName(), table.getTableName(), table);

        Table alteredTable = catalogToHiveConverter.convertTable(
                glueClient.getTable(new GetTableRequest()
                        .withCatalogId(anotherCatalogId)
                        .withDatabaseName(databaseInAnotherCatalog.getName())
                        .withName(table.getTableName())
                ).getTable(), database.getName());
        //time is updated on service side and can be different from what we sent
        table.setCreateTime(alteredTable.getCreateTime());
        table.setLastAccessTime(alteredTable.getLastAccessTime());
        assertEquals(table, alteredTable);
    }

    @Test
    public void dropTable() throws TException {
        metastoreClient.dropTable(database.getName(), table.getTableName());

        expectedException.expect(EntityNotFoundException.class);
        glueClient.getTable(new GetTableRequest()
                .withCatalogId(anotherCatalogId)
                .withDatabaseName(databaseInAnotherCatalog.getName())
                .withName(table.getTableName())
        );
    }
}
