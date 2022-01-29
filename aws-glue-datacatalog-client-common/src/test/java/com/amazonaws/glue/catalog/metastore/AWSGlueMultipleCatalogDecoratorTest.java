package com.amazonaws.glue.catalog.metastore;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.BatchCreatePartitionRequest;
import com.amazonaws.services.glue.model.BatchCreatePartitionResult;
import com.amazonaws.services.glue.model.BatchDeletePartitionRequest;
import com.amazonaws.services.glue.model.BatchDeletePartitionResult;
import com.amazonaws.services.glue.model.BatchDeleteTableRequest;
import com.amazonaws.services.glue.model.BatchDeleteTableResult;
import com.amazonaws.services.glue.model.BatchGetPartitionRequest;
import com.amazonaws.services.glue.model.BatchGetPartitionResult;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.CreateDatabaseResult;
import com.amazonaws.services.glue.model.CreatePartitionRequest;
import com.amazonaws.services.glue.model.CreatePartitionResult;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.CreateTableResult;
import com.amazonaws.services.glue.model.CreateUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.CreateUserDefinedFunctionResult;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.DeleteDatabaseResult;
import com.amazonaws.services.glue.model.DeletePartitionRequest;
import com.amazonaws.services.glue.model.DeletePartitionResult;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.DeleteTableResult;
import com.amazonaws.services.glue.model.DeleteUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.DeleteUserDefinedFunctionResult;
import com.amazonaws.services.glue.model.ErrorDetail;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.amazonaws.services.glue.model.GetDatabaseResult;
import com.amazonaws.services.glue.model.GetPartitionRequest;
import com.amazonaws.services.glue.model.GetPartitionResult;
import com.amazonaws.services.glue.model.GetPartitionsRequest;
import com.amazonaws.services.glue.model.GetPartitionsResult;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.GetTableResult;
import com.amazonaws.services.glue.model.GetTableVersionsRequest;
import com.amazonaws.services.glue.model.GetTableVersionsResult;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionResult;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionsRequest;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionsResult;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.PartitionError;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.PartitionValueList;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.TableError;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.TableVersion;
import com.amazonaws.services.glue.model.UpdateDatabaseRequest;
import com.amazonaws.services.glue.model.UpdateDatabaseResult;
import com.amazonaws.services.glue.model.UpdatePartitionRequest;
import com.amazonaws.services.glue.model.UpdatePartitionResult;
import com.amazonaws.services.glue.model.UpdateTableRequest;
import com.amazonaws.services.glue.model.UpdateTableResult;
import com.amazonaws.services.glue.model.UpdateUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.UpdateUserDefinedFunctionResult;
import com.amazonaws.services.glue.model.UserDefinedFunction;
import com.amazonaws.services.glue.model.UserDefinedFunctionInput;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class AWSGlueMultipleCatalogDecoratorTest {
    private static final String TABLE_NAME = "test_table";
    private static final String CURRENT_CATALOG_ID = "catalog_id";

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { "/", "test_database", "test_database", CURRENT_CATALOG_ID },
                { "@", "another_catalog@test_database", "test_database", "another_catalog" },
                { "/-/", "another_catalog/-/test_database", "test_database", "another_catalog" },
                { "a", "default", "default", CURRENT_CATALOG_ID }
        });
    }

    private AWSGlue glueClient;
    private AWSGlueMultipleCatalogDecorator decorator;
    private String catalogSeparator;
    private String externalDbName;
    private String internalDbName;
    private String internalCatalogId;

    public AWSGlueMultipleCatalogDecoratorTest(String catalogSeparator,
                                               String externalDbName,
                                               String internalDbName,
                                               String internalCatalogId) {
        this.catalogSeparator = catalogSeparator;
        this.externalDbName = externalDbName;
        this.internalDbName = internalDbName;
        this.internalCatalogId = internalCatalogId;
    }

    @Before
    public void setup() throws Exception {
        glueClient = mock(AWSGlue.class);
        decorator = new AWSGlueMultipleCatalogDecorator(glueClient, catalogSeparator);
    }

    @Test
    public void testBatchCreatePartition() throws Exception {
        BatchCreatePartitionRequest externalRequest = new BatchCreatePartitionRequest()
                .withCatalogId(CURRENT_CATALOG_ID)
                .withDatabaseName(externalDbName)
                .withTableName(TABLE_NAME)
                .withPartitionInputList(
                        new PartitionInput().withValues("val1", "val2"),
                        new PartitionInput().withValues("val3", "val4")
                );
        BatchCreatePartitionRequest internalRequest = externalRequest.clone()
                .withCatalogId(internalCatalogId)
                .withDatabaseName(internalDbName);

        BatchCreatePartitionResult internalResult = new BatchCreatePartitionResult()
                .withErrors(new PartitionError()
                        .withPartitionValues("val1", "val2")
                        .withErrorDetail(new ErrorDetail()
                                .withErrorCode("test_code")
                                .withErrorMessage("test message")
                        )
                );
        when(glueClient.batchCreatePartition(any())).thenReturn(internalResult);

        BatchCreatePartitionResult externalResult = decorator.batchCreatePartition(externalRequest);

        assertEquals(internalResult, externalResult);
        verify(glueClient).batchCreatePartition(internalRequest);
    }

    @Test
    public void testBatchDeletePartition() throws Exception {
        BatchDeletePartitionRequest externalRequest = new BatchDeletePartitionRequest()
                .withCatalogId(CURRENT_CATALOG_ID)
                .withDatabaseName(externalDbName)
                .withTableName(TABLE_NAME)
                .withPartitionsToDelete(
                        new PartitionValueList().withValues("val1", "val2"),
                        new PartitionValueList().withValues("val3", "val4")
                );
        BatchDeletePartitionRequest internalRequest = externalRequest.clone()
                .withCatalogId(internalCatalogId)
                .withDatabaseName(internalDbName);

        BatchDeletePartitionResult internalResult = new BatchDeletePartitionResult()
                .withErrors(new PartitionError()
                        .withPartitionValues("val1", "val2")
                        .withErrorDetail(new ErrorDetail()
                                .withErrorCode("test_code")
                                .withErrorMessage("test message")
                        )
                );
        when(glueClient.batchDeletePartition(any())).thenReturn(internalResult);

        BatchDeletePartitionResult externalResult = decorator.batchDeletePartition(externalRequest);

        assertEquals(internalResult, externalResult);
        verify(glueClient).batchDeletePartition(internalRequest);
    }

    @Test
    public void testBatchDeleteTable() throws Exception {
        BatchDeleteTableRequest externalRequest = new BatchDeleteTableRequest()
                .withCatalogId(CURRENT_CATALOG_ID)
                .withDatabaseName(externalDbName)
                .withTablesToDelete(TABLE_NAME);
        BatchDeleteTableRequest internalRequest = externalRequest.clone()
                .withCatalogId(internalCatalogId)
                .withDatabaseName(internalDbName);

        BatchDeleteTableResult internalResult = new BatchDeleteTableResult()
                .withErrors(new TableError()
                        .withTableName(TABLE_NAME)
                        .withErrorDetail(new ErrorDetail()
                                .withErrorCode("test_code")
                                .withErrorMessage("test message")
                        )
                );
        when(glueClient.batchDeleteTable(any())).thenReturn(internalResult);

        BatchDeleteTableResult externalResult = decorator.batchDeleteTable(externalRequest);

        assertEquals(internalResult, externalResult);
        verify(glueClient).batchDeleteTable(internalRequest);
    }

    @Test
    public void testBatchGetPartition() throws Exception {
        BatchGetPartitionRequest externalRequest = new BatchGetPartitionRequest()
                .withCatalogId(CURRENT_CATALOG_ID)
                .withDatabaseName(externalDbName)
                .withTableName(TABLE_NAME)
                .withPartitionsToGet(
                        new PartitionValueList().withValues("val1", "val2"),
                        new PartitionValueList().withValues("val3", "val4")
                );
        BatchGetPartitionRequest internalRequest = externalRequest.clone()
                .withCatalogId(internalCatalogId)
                .withDatabaseName(internalDbName);

        BatchGetPartitionResult internalResult = new BatchGetPartitionResult()
                .withPartitions(new Partition()
                        .withCreationTime(new Date())
                        .withDatabaseName(internalDbName)
                        .withLastAccessTime(new Date())
                        .withLastAnalyzedTime(new Date())
                        .withTableName(TABLE_NAME)
                        .withValues("val1", "val2"))
                .withUnprocessedKeys(new PartitionValueList().withValues("val3", "val4"));
        when(glueClient.batchGetPartition(any())).thenReturn(internalResult);

        BatchGetPartitionResult externalResult = decorator.batchGetPartition(externalRequest);

        BatchGetPartitionResult expectedExternalResult = internalResult.clone();
        expectedExternalResult.getPartitions().forEach(
                partition -> partition.setDatabaseName(externalDbName));
        assertEquals(expectedExternalResult, externalResult);
        verify(glueClient).batchGetPartition(internalRequest);
    }

    @Test
    public void testCreateDatabase() throws Exception {
        CreateDatabaseRequest externalRequest = new CreateDatabaseRequest()
                .withCatalogId(CURRENT_CATALOG_ID)
                .withDatabaseInput(new DatabaseInput()
                        .withDescription("Database description")
                        .withLocationUri("s3://db_path")
                        .withName(externalDbName));
        CreateDatabaseRequest internalRequest = externalRequest.clone()
                .withCatalogId(internalCatalogId)
                .withDatabaseInput(externalRequest.getDatabaseInput().clone().withName(internalDbName));

        CreateDatabaseResult internalResult = new CreateDatabaseResult();
        when(glueClient.createDatabase(any())).thenReturn(internalResult);

        CreateDatabaseResult externalResult = decorator.createDatabase(externalRequest);

        assertEquals(internalResult, externalResult);
        verify(glueClient).createDatabase(internalRequest);
    }

    @Test
    public void testCreatePartition() throws Exception {
        CreatePartitionRequest externalRequest = new CreatePartitionRequest()
                .withCatalogId(CURRENT_CATALOG_ID)
                .withDatabaseName(externalDbName)
                .withTableName(TABLE_NAME)
                .withPartitionInput(new PartitionInput().withValues("val1", "val2"));
        CreatePartitionRequest internalRequest = externalRequest.clone()
                .withCatalogId(internalCatalogId)
                .withDatabaseName(internalDbName);

        CreatePartitionResult internalResult = new CreatePartitionResult();
        when(glueClient.createPartition(any())).thenReturn(internalResult);

        CreatePartitionResult externalResult = decorator.createPartition(externalRequest);

        assertEquals(internalResult, externalResult);
        verify(glueClient).createPartition(internalRequest);
    }

    @Test
    public void testCreateTable() throws Exception {
        CreateTableRequest externalRequest = new CreateTableRequest()
                .withCatalogId(CURRENT_CATALOG_ID)
                .withDatabaseName(externalDbName)
                .withTableInput(new TableInput()
                        .withName(TABLE_NAME)
                        .withStorageDescriptor(new StorageDescriptor()
                                .withLocation("s3://some_path")
                                .withColumns(new Column().withName("col1").withType("string")))
                        .withDescription("Table description")
                );
        CreateTableRequest internalRequest = externalRequest.clone()
                .withCatalogId(internalCatalogId)
                .withDatabaseName(internalDbName);

        CreateTableResult internalResult = new CreateTableResult();
        when(glueClient.createTable(any())).thenReturn(internalResult);

        CreateTableResult externalResult = decorator.createTable(externalRequest);

        assertEquals(internalResult, externalResult);
        verify(glueClient).createTable(internalRequest);
    }

    @Test
    public void testCreateUserDefinedFunction() throws Exception {
        CreateUserDefinedFunctionRequest externalRequest = new CreateUserDefinedFunctionRequest()
                .withCatalogId(CURRENT_CATALOG_ID)
                .withDatabaseName(externalDbName)
                .withFunctionInput(new UserDefinedFunctionInput()
                        .withFunctionName("test_function")
                        .withClassName("com.amazon.glue.TestFunction"));
        CreateUserDefinedFunctionRequest internalRequest = externalRequest.clone()
                .withCatalogId(internalCatalogId)
                .withDatabaseName(internalDbName);

        CreateUserDefinedFunctionResult internalResult = new CreateUserDefinedFunctionResult();
        when(glueClient.createUserDefinedFunction(any())).thenReturn(internalResult);

        CreateUserDefinedFunctionResult externalResult = decorator.createUserDefinedFunction(externalRequest);

        assertEquals(internalResult, externalResult);
        verify(glueClient).createUserDefinedFunction(internalRequest);
    }

    @Test
    public void testDeleteDatabase() throws Exception {
        DeleteDatabaseRequest externalRequest = new DeleteDatabaseRequest()
                .withCatalogId(CURRENT_CATALOG_ID)
                .withName(externalDbName);
        DeleteDatabaseRequest internalRequest = externalRequest.clone()
                .withCatalogId(internalCatalogId)
                .withName(internalDbName);

        DeleteDatabaseResult internalResult = new DeleteDatabaseResult();
        when(glueClient.deleteDatabase(any())).thenReturn(internalResult);

        DeleteDatabaseResult externalResult = decorator.deleteDatabase(externalRequest);

        assertEquals(internalResult, externalResult);
        verify(glueClient).deleteDatabase(internalRequest);
    }

    @Test
    public void testDeletePartition() throws Exception {
        DeletePartitionRequest externalRequest = new DeletePartitionRequest()
                .withCatalogId(CURRENT_CATALOG_ID)
                .withDatabaseName(externalDbName)
                .withTableName(TABLE_NAME)
                .withPartitionValues("val1", "val2");
        DeletePartitionRequest internalRequest = externalRequest.clone()
                .withCatalogId(internalCatalogId)
                .withDatabaseName(internalDbName);

        DeletePartitionResult internalResult = new DeletePartitionResult();
        when(glueClient.deletePartition(any())).thenReturn(internalResult);

        DeletePartitionResult externalResult = decorator.deletePartition(externalRequest);

        assertEquals(internalResult, externalResult);
        verify(glueClient).deletePartition(internalRequest);
    }

    @Test
    public void testDeleteTable() throws Exception {
        DeleteTableRequest externalRequest = new DeleteTableRequest()
                .withCatalogId(CURRENT_CATALOG_ID)
                .withDatabaseName(externalDbName)
                .withName(TABLE_NAME);
        DeleteTableRequest internalRequest = externalRequest.clone()
                .withCatalogId(internalCatalogId)
                .withDatabaseName(internalDbName);

        DeleteTableResult internalResult = new DeleteTableResult();
        when(glueClient.deleteTable(any())).thenReturn(internalResult);

        DeleteTableResult externalResult = decorator.deleteTable(externalRequest);

        assertEquals(internalResult, externalResult);
        verify(glueClient).deleteTable(internalRequest);
    }

    @Test
    public void testDeleteUserDefinedFunction() throws Exception {
        DeleteUserDefinedFunctionRequest externalRequest = new DeleteUserDefinedFunctionRequest()
                .withCatalogId(CURRENT_CATALOG_ID)
                .withDatabaseName(externalDbName)
                .withFunctionName("test_function");
        DeleteUserDefinedFunctionRequest internalRequest = externalRequest.clone()
                .withCatalogId(internalCatalogId)
                .withDatabaseName(internalDbName);

        DeleteUserDefinedFunctionResult internalResult = new DeleteUserDefinedFunctionResult();
        when(glueClient.deleteUserDefinedFunction(any())).thenReturn(internalResult);

        DeleteUserDefinedFunctionResult externalResult = decorator.deleteUserDefinedFunction(externalRequest);

        assertEquals(internalResult, externalResult);
        verify(glueClient).deleteUserDefinedFunction(internalRequest);
    }

    @Test
    public void testGetDatabase() throws Exception {
        GetDatabaseRequest externalRequest = new GetDatabaseRequest()
                .withCatalogId(CURRENT_CATALOG_ID)
                .withName(externalDbName);
        GetDatabaseRequest internalRequest = externalRequest.clone()
                .withCatalogId(internalCatalogId)
                .withName(internalDbName);

        GetDatabaseResult internalResult = new GetDatabaseResult()
                .withDatabase(new Database()
                        .withName(internalDbName)
                        .withCreateTime(new Date())
                        .withDescription("Database description")
                        .withLocationUri("s3://db_path"));
        when(glueClient.getDatabase(any())).thenReturn(internalResult);

        GetDatabaseResult externalResult = decorator.getDatabase(externalRequest);

        GetDatabaseResult expectedExternalResult = internalResult.clone()
                .withDatabase(internalResult.getDatabase().clone()
                        .withName(externalDbName));
        assertEquals(expectedExternalResult, externalResult);
        verify(glueClient).getDatabase(internalRequest);
    }

    @Test
    public void testGetPartition() throws Exception {
        GetPartitionRequest externalRequest = new GetPartitionRequest()
                .withCatalogId(CURRENT_CATALOG_ID)
                .withDatabaseName(externalDbName)
                .withTableName(TABLE_NAME)
                .withPartitionValues("val1", "val2");
        GetPartitionRequest internalRequest = externalRequest.clone()
                .withCatalogId(internalCatalogId)
                .withDatabaseName(internalDbName);

        GetPartitionResult internalResult = new GetPartitionResult()
                .withPartition(new Partition()
                        .withDatabaseName(internalDbName)
                        .withTableName(TABLE_NAME)
                        .withCreationTime(new Date())
                        .withValues("val1", "val2"));
        when(glueClient.getPartition(any())).thenReturn(internalResult);

        GetPartitionResult externalResult = decorator.getPartition(externalRequest);

        GetPartitionResult expectedExternalResult = internalResult.clone()
                .withPartition(internalResult.getPartition().clone()
                        .withDatabaseName(externalDbName));
        assertEquals(expectedExternalResult, externalResult);
        verify(glueClient).getPartition(internalRequest);
    }

    @Test
    public void testGetPartitions() throws Exception {
        GetPartitionsRequest externalRequest = new GetPartitionsRequest()
                .withCatalogId(CURRENT_CATALOG_ID)
                .withDatabaseName(externalDbName)
                .withTableName(TABLE_NAME)
                .withExpression("f1='val1' and f2='val2'");
        GetPartitionsRequest internalRequest = externalRequest.clone()
                .withCatalogId(internalCatalogId)
                .withDatabaseName(internalDbName);

        GetPartitionsResult internalResult = new GetPartitionsResult()
                .withPartitions(new Partition()
                        .withDatabaseName(internalDbName)
                        .withTableName(TABLE_NAME)
                        .withCreationTime(new Date())
                        .withValues("val1", "val2"));
        when(glueClient.getPartitions(any())).thenReturn(internalResult);

        GetPartitionsResult externalResult = decorator.getPartitions(externalRequest);

        GetPartitionsResult expectedExternalResult = internalResult.clone()
                .withPartitions(internalResult.getPartitions().stream().map(
                        partition -> partition.clone().withDatabaseName(externalDbName)
                ).collect(Collectors.toList()));
        assertEquals(expectedExternalResult, externalResult);
        verify(glueClient).getPartitions(internalRequest);
    }

    @Test
    public void testGetTable() throws Exception {
        GetTableRequest externalRequest = new GetTableRequest()
                .withCatalogId(CURRENT_CATALOG_ID)
                .withDatabaseName(externalDbName)
                .withName(TABLE_NAME);
        GetTableRequest internalRequest = externalRequest.clone()
                .withCatalogId(internalCatalogId)
                .withDatabaseName(internalDbName);

        GetTableResult internalResult = new GetTableResult()
                .withTable(new Table()
                        .withDatabaseName(internalDbName)
                        .withName(TABLE_NAME)
                        .withDescription("Table description")
                        .withPartitionKeys(new Column().withName("col1").withType("string")));
        when(glueClient.getTable(any())).thenReturn(internalResult);

        GetTableResult externalResult = decorator.getTable(externalRequest);

        GetTableResult expectedExternalResult = internalResult.clone()
                .withTable(internalResult.getTable().clone()
                        .withDatabaseName(externalDbName));
        assertEquals(expectedExternalResult, externalResult);
        verify(glueClient).getTable(internalRequest);
    }

    @Test
    public void testGetTableVersions() throws Exception {
        GetTableVersionsRequest externalRequest = new GetTableVersionsRequest()
                .withCatalogId(CURRENT_CATALOG_ID)
                .withDatabaseName(externalDbName)
                .withTableName(TABLE_NAME);
        GetTableVersionsRequest internalRequest = externalRequest.clone()
                .withCatalogId(internalCatalogId)
                .withDatabaseName(internalDbName);

        GetTableVersionsResult internalResult = new GetTableVersionsResult()
                .withTableVersions(new TableVersion()
                        .withVersionId("1")
                        .withTable(new Table()
                                .withDatabaseName(internalDbName)
                                .withName(TABLE_NAME)
                                .withDescription("Table description")
                                .withPartitionKeys(new Column().withName("col1").withType("string"))));
        when(glueClient.getTableVersions(any())).thenReturn(internalResult);

        GetTableVersionsResult externalResult = decorator.getTableVersions(externalRequest);

        GetTableVersionsResult expectedExternalResult = internalResult.clone()
                .withTableVersions(new TableVersion()
                        .withVersionId("1")
                        .withTable(internalResult.getTableVersions().get(0).getTable().clone()
                                .withDatabaseName(externalDbName)));
        assertEquals(expectedExternalResult, externalResult);
        verify(glueClient).getTableVersions(internalRequest);
    }

    @Test
    public void testGetTables() throws Exception {
        GetTablesRequest externalRequest = new GetTablesRequest()
                .withCatalogId(CURRENT_CATALOG_ID)
                .withDatabaseName(externalDbName)
                .withExpression("*");
        GetTablesRequest internalRequest = externalRequest.clone()
                .withCatalogId(internalCatalogId)
                .withDatabaseName(internalDbName);

        GetTablesResult internalResult = new GetTablesResult()
                .withTableList(new Table()
                        .withDatabaseName(internalDbName)
                        .withName(TABLE_NAME)
                        .withDescription("Table description")
                        .withPartitionKeys(new Column().withName("col1").withType("string")));
        when(glueClient.getTables(any())).thenReturn(internalResult);

        GetTablesResult externalResult = decorator.getTables(externalRequest);

        GetTablesResult expectedExternalResult = internalResult.clone()
                .withTableList(internalResult.getTableList().stream().map(
                        table -> table.clone().withDatabaseName(externalDbName)
                ).collect(Collectors.toList()));
        assertEquals(expectedExternalResult, externalResult);
        verify(glueClient).getTables(internalRequest);
    }

    @Test
    public void testGetUserDefinedFunction() throws Exception {
        GetUserDefinedFunctionRequest externalRequest = new GetUserDefinedFunctionRequest()
                .withCatalogId(CURRENT_CATALOG_ID)
                .withDatabaseName(externalDbName)
                .withFunctionName("test_function");
        GetUserDefinedFunctionRequest internalRequest = externalRequest.clone()
                .withCatalogId(internalCatalogId)
                .withDatabaseName(internalDbName);

        GetUserDefinedFunctionResult internalResult = new GetUserDefinedFunctionResult()
                .withUserDefinedFunction(new UserDefinedFunction()
                        .withFunctionName("test_function")
                        .withClassName("org.amazon.glue.TestFunction")
                        .withCreateTime(new Date()));
        when(glueClient.getUserDefinedFunction(any())).thenReturn(internalResult);

        GetUserDefinedFunctionResult externalResult = decorator.getUserDefinedFunction(externalRequest);

        assertEquals(internalResult, externalResult);
        verify(glueClient).getUserDefinedFunction(internalRequest);
    }

    @Test
    public void testGetUserDefinedFunctions() throws Exception {
        GetUserDefinedFunctionsRequest externalRequest = new GetUserDefinedFunctionsRequest()
                .withCatalogId(CURRENT_CATALOG_ID)
                .withDatabaseName(externalDbName)
                .withPattern("*");
        GetUserDefinedFunctionsRequest internalRequest = externalRequest.clone()
                .withCatalogId(internalCatalogId)
                .withDatabaseName(internalDbName);

        GetUserDefinedFunctionsResult internalResult = new GetUserDefinedFunctionsResult()
                .withUserDefinedFunctions(new UserDefinedFunction()
                        .withFunctionName("test_function")
                        .withClassName("org.amazon.glue.TestFunction")
                        .withCreateTime(new Date()));
        when(glueClient.getUserDefinedFunctions(any())).thenReturn(internalResult);

        GetUserDefinedFunctionsResult externalResult = decorator.getUserDefinedFunctions(externalRequest);

        assertEquals(internalResult, externalResult);
        verify(glueClient).getUserDefinedFunctions(internalRequest);
    }

    @Test
    public void testUpdateDatabase() throws Exception {
        String dbName = externalDbName;
        UpdateDatabaseRequest externalRequest = new UpdateDatabaseRequest()
                .withCatalogId(CURRENT_CATALOG_ID)
                .withName(dbName)
                .withDatabaseInput(new DatabaseInput()
                        .withName(dbName)
                        .withLocationUri("s3://new_path")
                        .withDescription("Database description")
                );
        UpdateDatabaseRequest internalRequest = externalRequest.clone()
                .withCatalogId(internalCatalogId)
                .withName(internalDbName)
                .withDatabaseInput(externalRequest.getDatabaseInput().clone().withName(internalDbName));

        UpdateDatabaseResult internalResult = new UpdateDatabaseResult();
        when(glueClient.updateDatabase(any())).thenReturn(internalResult);

        UpdateDatabaseResult externalResult = decorator.updateDatabase(externalRequest);

        assertEquals(internalResult, externalResult);
        verify(glueClient).updateDatabase(internalRequest);
    }

    @Test
    public void testUpdatePartition() throws Exception {
        UpdatePartitionRequest externalRequest = new UpdatePartitionRequest()
                .withCatalogId(CURRENT_CATALOG_ID)
                .withDatabaseName(externalDbName)
                .withTableName(TABLE_NAME)
                .withPartitionValueList("val1")
                .withPartitionInput(new PartitionInput()
                        .withValues("val1")
                        .withStorageDescriptor(new StorageDescriptor()
                                .withColumns(new Column().withName("col1").withType("string"))
                                .withLocation("s3://new_location")
                        )
                );
        UpdatePartitionRequest internalRequest = externalRequest.clone()
                .withCatalogId(internalCatalogId)
                .withDatabaseName(internalDbName);

        UpdatePartitionResult internalResult = new UpdatePartitionResult();
        when(glueClient.updatePartition(any())).thenReturn(internalResult);

        UpdatePartitionResult externalResult = decorator.updatePartition(externalRequest);

        assertEquals(internalResult, externalResult);
        verify(glueClient).updatePartition(internalRequest);
    }

    @Test
    public void testUpdateTable() throws Exception {
        UpdateTableRequest externalRequest = new UpdateTableRequest()
                .withCatalogId(CURRENT_CATALOG_ID)
                .withDatabaseName(externalDbName)
                .withTableInput(new TableInput()
                        .withName(TABLE_NAME)
                        .withDescription("Table description")
                        .withPartitionKeys(new Column().withName("col1").withType("string"))
                );
        UpdateTableRequest internalRequest = externalRequest.clone()
                .withCatalogId(internalCatalogId)
                .withDatabaseName(internalDbName);

        UpdateTableResult internalResult = new UpdateTableResult();
        when(glueClient.updateTable(any())).thenReturn(internalResult);

        UpdateTableResult externalResult = decorator.updateTable(externalRequest);

        assertEquals(internalResult, externalResult);
        verify(glueClient).updateTable(internalRequest);
    }

    @Test
    public void testUpdateUserDefinedFunction() throws Exception {
        UpdateUserDefinedFunctionRequest externalRequest = new UpdateUserDefinedFunctionRequest()
                .withCatalogId(CURRENT_CATALOG_ID)
                .withDatabaseName(externalDbName)
                .withFunctionName("test_function")
                .withFunctionInput(new UserDefinedFunctionInput()
                        .withFunctionName("test_function")
                        .withClassName("com.amazon.glue.NewTestFunction")
                );
        UpdateUserDefinedFunctionRequest internalRequest = externalRequest.clone()
                .withCatalogId(internalCatalogId)
                .withDatabaseName(internalDbName);

        UpdateUserDefinedFunctionResult internalResult = new UpdateUserDefinedFunctionResult();
        when(glueClient.updateUserDefinedFunction(any())).thenReturn(internalResult);

        UpdateUserDefinedFunctionResult externalResult = decorator.updateUserDefinedFunction(externalRequest);

        assertEquals(internalResult, externalResult);
        verify(glueClient).updateUserDefinedFunction(internalRequest);
    }
}
