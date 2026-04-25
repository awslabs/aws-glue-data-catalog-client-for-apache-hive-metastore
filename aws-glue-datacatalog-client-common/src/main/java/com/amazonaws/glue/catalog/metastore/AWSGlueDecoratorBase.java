package com.amazonaws.glue.catalog.metastore;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.BatchCreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchCreatePartitionResponse;
import software.amazon.awssdk.services.glue.model.BatchDeletePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchDeletePartitionResponse;
import software.amazon.awssdk.services.glue.model.BatchDeleteTableRequest;
import software.amazon.awssdk.services.glue.model.BatchDeleteTableResponse;
import software.amazon.awssdk.services.glue.model.BatchGetPartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchGetPartitionResponse;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateDatabaseResponse;
import software.amazon.awssdk.services.glue.model.CreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.CreatePartitionResponse;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.CreateTableResponse;
import software.amazon.awssdk.services.glue.model.CreateUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.CreateUserDefinedFunctionResponse;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseResponse;
import software.amazon.awssdk.services.glue.model.DeletePartitionRequest;
import software.amazon.awssdk.services.glue.model.DeletePartitionResponse;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableResponse;
import software.amazon.awssdk.services.glue.model.DeleteUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.DeleteUserDefinedFunctionResponse;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionResponse;
import software.amazon.awssdk.services.glue.model.GetPartitionsRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GetTableVersionsRequest;
import software.amazon.awssdk.services.glue.model.GetTableVersionsResponse;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionResponse;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionsRequest;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionsResponse;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseResponse;
import software.amazon.awssdk.services.glue.model.UpdatePartitionRequest;
import software.amazon.awssdk.services.glue.model.UpdatePartitionResponse;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableResponse;
import software.amazon.awssdk.services.glue.model.UpdateUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.UpdateUserDefinedFunctionResponse;

/**
 * Simplified composition wrapper for GlueClient. Instead of implementing the full AWSGlue interface
 * (which doesn't exist in SDK v2), this wraps a GlueClient and exposes only the metastore-relevant
 * operations as delegate methods that subclasses can override.
 */
public class AWSGlueDecoratorBase {
    private final GlueClient decoratedClient;

    public AWSGlueDecoratorBase(GlueClient glueClient) {
        this.decoratedClient = glueClient;
    }

    protected GlueClient getDelegate() {
        return decoratedClient;
    }

    public BatchCreatePartitionResponse batchCreatePartition(BatchCreatePartitionRequest request) {
        return decoratedClient.batchCreatePartition(request);
    }

    public BatchDeletePartitionResponse batchDeletePartition(BatchDeletePartitionRequest request) {
        return decoratedClient.batchDeletePartition(request);
    }

    public BatchDeleteTableResponse batchDeleteTable(BatchDeleteTableRequest request) {
        return decoratedClient.batchDeleteTable(request);
    }

    public BatchGetPartitionResponse batchGetPartition(BatchGetPartitionRequest request) {
        return decoratedClient.batchGetPartition(request);
    }

    public CreateDatabaseResponse createDatabase(CreateDatabaseRequest request) {
        return decoratedClient.createDatabase(request);
    }

    public CreatePartitionResponse createPartition(CreatePartitionRequest request) {
        return decoratedClient.createPartition(request);
    }

    public CreateTableResponse createTable(CreateTableRequest request) {
        return decoratedClient.createTable(request);
    }

    public CreateUserDefinedFunctionResponse createUserDefinedFunction(CreateUserDefinedFunctionRequest request) {
        return decoratedClient.createUserDefinedFunction(request);
    }

    public DeleteDatabaseResponse deleteDatabase(DeleteDatabaseRequest request) {
        return decoratedClient.deleteDatabase(request);
    }

    public DeletePartitionResponse deletePartition(DeletePartitionRequest request) {
        return decoratedClient.deletePartition(request);
    }

    public DeleteTableResponse deleteTable(DeleteTableRequest request) {
        return decoratedClient.deleteTable(request);
    }

    public DeleteUserDefinedFunctionResponse deleteUserDefinedFunction(DeleteUserDefinedFunctionRequest request) {
        return decoratedClient.deleteUserDefinedFunction(request);
    }

    public GetDatabaseResponse getDatabase(GetDatabaseRequest request) {
        return decoratedClient.getDatabase(request);
    }

    public GetDatabasesResponse getDatabases(GetDatabasesRequest request) {
        return decoratedClient.getDatabases(request);
    }

    public GetPartitionResponse getPartition(GetPartitionRequest request) {
        return decoratedClient.getPartition(request);
    }

    public GetPartitionsResponse getPartitions(GetPartitionsRequest request) {
        return decoratedClient.getPartitions(request);
    }

    public GetTableResponse getTable(GetTableRequest request) {
        return decoratedClient.getTable(request);
    }

    public GetTablesResponse getTables(GetTablesRequest request) {
        return decoratedClient.getTables(request);
    }

    public GetTableVersionsResponse getTableVersions(GetTableVersionsRequest request) {
        return decoratedClient.getTableVersions(request);
    }

    public GetUserDefinedFunctionResponse getUserDefinedFunction(GetUserDefinedFunctionRequest request) {
        return decoratedClient.getUserDefinedFunction(request);
    }

    public GetUserDefinedFunctionsResponse getUserDefinedFunctions(GetUserDefinedFunctionsRequest request) {
        return decoratedClient.getUserDefinedFunctions(request);
    }

    public UpdateDatabaseResponse updateDatabase(UpdateDatabaseRequest request) {
        return decoratedClient.updateDatabase(request);
    }

    public UpdatePartitionResponse updatePartition(UpdatePartitionRequest request) {
        return decoratedClient.updatePartition(request);
    }

    public UpdateTableResponse updateTable(UpdateTableRequest request) {
        return decoratedClient.updateTable(request);
    }

    public UpdateUserDefinedFunctionResponse updateUserDefinedFunction(UpdateUserDefinedFunctionRequest request) {
        return decoratedClient.updateUserDefinedFunction(request);
    }

    public void close() {
        decoratedClient.close();
    }
}
