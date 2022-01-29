package com.amazonaws.glue.catalog.metastore.integrationtest;

import com.amazonaws.glue.catalog.converters.BaseCatalogToHiveConverter;
import com.amazonaws.glue.catalog.converters.CatalogToHiveConverter;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashSet;
import java.util.Set;

import static com.amazonaws.glue.catalog.util.TestObjects.getTestDatabase;
import static com.amazonaws.glue.catalog.util.TestObjects.getCatalogTestFunction;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MultipleCatalogUserDefinedFunctionIntegrationTest extends MultipleCatalogIntegrationTestBase {
    private Database databaseInAnotherCatalog;
    private Database database;
    private Function function1;
    private Function function2;
    private Function function3;
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

        function1 = catalogToHiveConverter.convertFunction(database.getName(), getCatalogTestFunction());
        metastoreClient.createFunction(function1);

        function2 = catalogToHiveConverter.convertFunction(database.getName(), getCatalogTestFunction());
        metastoreClient.createFunction(function2);

        function3 = catalogToHiveConverter.convertFunction(database.getName(), getCatalogTestFunction());
        metastoreClient.createFunction(function3);
    }

    @After
    public void clean() {
        glueClient.deleteDatabase(new DeleteDatabaseRequest().withName(database.getName()));
    }

    @Test
    public void getFunction() throws TException {
        Function createdFunction = metastoreClient.getFunction(database.getName(), function1.getFunctionName());
        function1.setCreateTime(createdFunction.getCreateTime()); //it can be changed during creation process
        assertEquals(function1, createdFunction);

        createdFunction = catalogToHiveConverter.convertFunction(database.getName(),
                glueClient.getUserDefinedFunction(new GetUserDefinedFunctionRequest()
                    .withCatalogId(anotherCatalogId)
                    .withDatabaseName(databaseInAnotherCatalog.getName())
                    .withFunctionName(function1.getFunctionName())
                ).getUserDefinedFunction()
        );
        assertEquals(function1, createdFunction);
    }

    @Test
    public void getFunctions() throws TException {
        Set<String> functionNames = new HashSet<>(metastoreClient.getFunctions(database.getName(), ".*"));
        assertEquals(3, functionNames.size());
        assertTrue(functionNames.contains(function1.getFunctionName()));
        assertTrue(functionNames.contains(function2.getFunctionName()));
        assertTrue(functionNames.contains(function3.getFunctionName()));
    }

    @Test
    public void updateFunction() throws TException {
        Function newFunction = function1.deepCopy();
        String newClassName = "newClassName";
        newFunction.setClassName(newClassName);
        metastoreClient.alterFunction(database.getName(), function1.getFunctionName(), newFunction);

        Function alteredFunction = catalogToHiveConverter.convertFunction(database.getName(),
                glueClient.getUserDefinedFunction(new GetUserDefinedFunctionRequest()
                        .withCatalogId(anotherCatalogId)
                        .withDatabaseName(databaseInAnotherCatalog.getName())
                        .withFunctionName(newFunction.getFunctionName())
                ).getUserDefinedFunction()
        );
        alteredFunction.setCreateTime(newFunction.getCreateTime());
        assertEquals(newFunction, alteredFunction);
    }

    @Test
    public void dropFunction() throws TException {
        metastoreClient.dropFunction(database.getName(), function1.getFunctionName());

        expectedException.expect(EntityNotFoundException.class);
        glueClient.getUserDefinedFunction(new GetUserDefinedFunctionRequest()
                .withCatalogId(anotherCatalogId)
                .withDatabaseName(databaseInAnotherCatalog.getName())
                .withFunctionName(function1.getFunctionName())
        );
    }
}
