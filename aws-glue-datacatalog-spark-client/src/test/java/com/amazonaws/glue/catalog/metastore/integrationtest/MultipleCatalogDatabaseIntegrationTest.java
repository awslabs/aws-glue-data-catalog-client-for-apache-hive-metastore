package com.amazonaws.glue.catalog.metastore.integrationtest;

import com.amazonaws.glue.catalog.converters.BaseCatalogToHiveConverter;
import com.amazonaws.glue.catalog.converters.CatalogToHiveConverter;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.UUID;

import static com.amazonaws.glue.catalog.util.TestObjects.getTestDatabase;
import static org.junit.Assert.assertEquals;

public class MultipleCatalogDatabaseIntegrationTest extends MultipleCatalogIntegrationTestBase {

    private Database databaseInAnotherCatalog;
    private Database database;
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
    }

    @After
    public void clean() {
        try {
            glueClient.deleteDatabase(new DeleteDatabaseRequest().withName(databaseInAnotherCatalog.getName()));
        } catch (EntityNotFoundException e) {
        }
    }

    @Test
    public void getDatabase() throws TException {
        Database createdDb = metastoreClient.getDatabase(database.getName());
        assertEquals(database, createdDb);

        createdDb = catalogToHiveConverter.convertDatabase(
                glueClient.getDatabase(new GetDatabaseRequest()
                        .withCatalogId(anotherCatalogId)
                        .withName(databaseInAnotherCatalog.getName())
                ).getDatabase());
        assertEquals(databaseInAnotherCatalog, createdDb);
    }

    @Test
    public void alterDatabase() throws TException {
        String newDescription = UUID.randomUUID().toString();
        database.setDescription(newDescription);
        databaseInAnotherCatalog.setDescription(newDescription);
        metastoreClient.alterDatabase(database.getName(), database);

        Database alteredDb = catalogToHiveConverter.convertDatabase(
                glueClient.getDatabase(new GetDatabaseRequest()
                        .withCatalogId(anotherCatalogId)
                        .withName(databaseInAnotherCatalog.getName())
                ).getDatabase());
        assertEquals(databaseInAnotherCatalog, alteredDb);
    }

    @Test
    public void dropDatabase() throws TException {
        metastoreClient.dropDatabase(database.getName());

        expectedException.expect(EntityNotFoundException.class);
        glueClient.getDatabase(new GetDatabaseRequest()
                .withCatalogId(anotherCatalogId)
                .withName(databaseInAnotherCatalog.getName())
        );
    }
}
