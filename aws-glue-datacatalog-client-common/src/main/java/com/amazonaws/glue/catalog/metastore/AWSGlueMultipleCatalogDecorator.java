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
import software.amazon.awssdk.services.glue.model.Partition;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseResponse;
import software.amazon.awssdk.services.glue.model.UpdatePartitionRequest;
import software.amazon.awssdk.services.glue.model.UpdatePartitionResponse;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableResponse;
import software.amazon.awssdk.services.glue.model.UpdateUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.UpdateUserDefinedFunctionResponse;
import com.google.common.base.Strings;

import java.util.stream.Collectors;


public class AWSGlueMultipleCatalogDecorator extends AWSGlueDecoratorBase {

    // We're not importing this from Hive's Warehouse class as the package name is changed between Hive 1.x and Hive 3.x
    private static final String DEFAULT_DATABASE_NAME = "default";

    private String catalogSeparator;

    public AWSGlueMultipleCatalogDecorator(GlueClient glueClient, String catalogSeparator) {
        super(glueClient);
        this.catalogSeparator = catalogSeparator;
    }

    private static class CatalogResult {
        final String catalogId;
        final String databaseName;

        CatalogResult(String catalogId, String databaseName) {
            this.catalogId = catalogId;
            this.databaseName = databaseName;
        }
    }

    private CatalogResult extractCatalog(String databaseName) {
        if (!Strings.isNullOrEmpty(this.catalogSeparator) && databaseName != null
            && !databaseName.equals(DEFAULT_DATABASE_NAME)) {
            int idx = databaseName.indexOf(this.catalogSeparator);
            if (idx >= 0) {
                return new CatalogResult(
                    databaseName.substring(0, idx),
                    databaseName.substring(idx + this.catalogSeparator.length()));
            }
        }
        return null;
    }

    @Override
    public BatchCreatePartitionResponse batchCreatePartition(BatchCreatePartitionRequest request) {
        CatalogResult cr = extractCatalog(request.databaseName());
        if (cr != null) {
            request = request.toBuilder().databaseName(cr.databaseName).catalogId(cr.catalogId).build();
        }
        return super.batchCreatePartition(request);
    }

    @Override
    public BatchDeletePartitionResponse batchDeletePartition(BatchDeletePartitionRequest request) {
        CatalogResult cr = extractCatalog(request.databaseName());
        if (cr != null) {
            request = request.toBuilder().databaseName(cr.databaseName).catalogId(cr.catalogId).build();
        }
        return super.batchDeletePartition(request);
    }

    @Override
    public BatchDeleteTableResponse batchDeleteTable(BatchDeleteTableRequest request) {
        CatalogResult cr = extractCatalog(request.databaseName());
        if (cr != null) {
            request = request.toBuilder().databaseName(cr.databaseName).catalogId(cr.catalogId).build();
        }
        return super.batchDeleteTable(request);
    }

    @Override
    public BatchGetPartitionResponse batchGetPartition(BatchGetPartitionRequest request) {
        String originalDatabaseName = request.databaseName();
        CatalogResult cr = extractCatalog(originalDatabaseName);
        if (cr != null) {
            request = request.toBuilder().databaseName(cr.databaseName).catalogId(cr.catalogId).build();
        }
        BatchGetPartitionResponse result = super.batchGetPartition(request);
        return result.toBuilder().partitions(
            result.partitions().stream()
                .map(p -> p.toBuilder().databaseName(originalDatabaseName).build())
                .collect(Collectors.toList())
        ).build();
    }

    @Override
    public CreateDatabaseResponse createDatabase(CreateDatabaseRequest request) {
        if (request.databaseInput() != null) {
            CatalogResult cr = extractCatalog(request.databaseInput().name());
            if (cr != null) {
                request = request.toBuilder()
                    .catalogId(cr.catalogId)
                    .databaseInput(request.databaseInput().toBuilder().name(cr.databaseName).build())
                    .build();
            }
        }
        return super.createDatabase(request);
    }

    @Override
    public CreatePartitionResponse createPartition(CreatePartitionRequest request) {
        CatalogResult cr = extractCatalog(request.databaseName());
        if (cr != null) {
            request = request.toBuilder().databaseName(cr.databaseName).catalogId(cr.catalogId).build();
        }
        return super.createPartition(request);
    }

    @Override
    public CreateTableResponse createTable(CreateTableRequest request) {
        CatalogResult cr = extractCatalog(request.databaseName());
        if (cr != null) {
            request = request.toBuilder().databaseName(cr.databaseName).catalogId(cr.catalogId).build();
        }
        return super.createTable(request);
    }

    @Override
    public CreateUserDefinedFunctionResponse createUserDefinedFunction(CreateUserDefinedFunctionRequest request) {
        CatalogResult cr = extractCatalog(request.databaseName());
        if (cr != null) {
            request = request.toBuilder().databaseName(cr.databaseName).catalogId(cr.catalogId).build();
        }
        return super.createUserDefinedFunction(request);
    }

    @Override
    public DeleteDatabaseResponse deleteDatabase(DeleteDatabaseRequest request) {
        CatalogResult cr = extractCatalog(request.name());
        if (cr != null) {
            request = request.toBuilder().name(cr.databaseName).catalogId(cr.catalogId).build();
        }
        return super.deleteDatabase(request);
    }

    @Override
    public DeletePartitionResponse deletePartition(DeletePartitionRequest request) {
        CatalogResult cr = extractCatalog(request.databaseName());
        if (cr != null) {
            request = request.toBuilder().databaseName(cr.databaseName).catalogId(cr.catalogId).build();
        }
        return super.deletePartition(request);
    }

    @Override
    public DeleteTableResponse deleteTable(DeleteTableRequest request) {
        CatalogResult cr = extractCatalog(request.databaseName());
        if (cr != null) {
            request = request.toBuilder().databaseName(cr.databaseName).catalogId(cr.catalogId).build();
        }
        return super.deleteTable(request);
    }

    @Override
    public DeleteUserDefinedFunctionResponse deleteUserDefinedFunction(DeleteUserDefinedFunctionRequest request) {
        CatalogResult cr = extractCatalog(request.databaseName());
        if (cr != null) {
            request = request.toBuilder().databaseName(cr.databaseName).catalogId(cr.catalogId).build();
        }
        return super.deleteUserDefinedFunction(request);
    }

    @Override
    public GetDatabaseResponse getDatabase(GetDatabaseRequest request) {
        String originalDatabaseName = request.name();
        CatalogResult cr = extractCatalog(originalDatabaseName);
        if (cr != null) {
            request = request.toBuilder().name(cr.databaseName).catalogId(cr.catalogId).build();
        }
        GetDatabaseResponse result = super.getDatabase(request);
        return result.toBuilder()
            .database(result.database().toBuilder().name(originalDatabaseName).build())
            .build();
    }

    @Override
    public GetPartitionResponse getPartition(GetPartitionRequest request) {
        String originalDatabaseName = request.databaseName();
        CatalogResult cr = extractCatalog(originalDatabaseName);
        if (cr != null) {
            request = request.toBuilder().databaseName(cr.databaseName).catalogId(cr.catalogId).build();
        }
        GetPartitionResponse result = super.getPartition(request);
        return result.toBuilder()
            .partition(result.partition().toBuilder().databaseName(originalDatabaseName).build())
            .build();
    }

    @Override
    public GetPartitionsResponse getPartitions(GetPartitionsRequest request) {
        String originalDatabaseName = request.databaseName();
        CatalogResult cr = extractCatalog(originalDatabaseName);
        if (cr != null) {
            request = request.toBuilder().databaseName(cr.databaseName).catalogId(cr.catalogId).build();
        }
        GetPartitionsResponse result = super.getPartitions(request);
        return result.toBuilder().partitions(
            result.partitions().stream()
                .map(p -> p.toBuilder().databaseName(originalDatabaseName).build())
                .collect(Collectors.toList())
        ).build();
    }

    @Override
    public GetTableResponse getTable(GetTableRequest request) {
        String originalDatabaseName = request.databaseName();
        CatalogResult cr = extractCatalog(originalDatabaseName);
        if (cr != null) {
            request = request.toBuilder().databaseName(cr.databaseName).catalogId(cr.catalogId).build();
        }
        GetTableResponse result = super.getTable(request);
        return result.toBuilder()
            .table(result.table().toBuilder().databaseName(originalDatabaseName).build())
            .build();
    }

    @Override
    public GetTableVersionsResponse getTableVersions(GetTableVersionsRequest request) {
        String originalDatabaseName = request.databaseName();
        CatalogResult cr = extractCatalog(originalDatabaseName);
        if (cr != null) {
            request = request.toBuilder().databaseName(cr.databaseName).catalogId(cr.catalogId).build();
        }
        GetTableVersionsResponse result = super.getTableVersions(request);
        return result.toBuilder().tableVersions(
            result.tableVersions().stream()
                .map(tv -> tv.toBuilder()
                    .table(tv.table().toBuilder().databaseName(originalDatabaseName).build())
                    .build())
                .collect(Collectors.toList())
        ).build();
    }

    @Override
    public GetTablesResponse getTables(GetTablesRequest request) {
        String originalDatabaseName = request.databaseName();
        CatalogResult cr = extractCatalog(originalDatabaseName);
        if (cr != null) {
            request = request.toBuilder().databaseName(cr.databaseName).catalogId(cr.catalogId).build();
        }
        GetTablesResponse result = super.getTables(request);
        return result.toBuilder().tableList(
            result.tableList().stream()
                .map(t -> t.toBuilder().databaseName(originalDatabaseName).build())
                .collect(Collectors.toList())
        ).build();
    }

    @Override
    public GetUserDefinedFunctionResponse getUserDefinedFunction(GetUserDefinedFunctionRequest request) {
        CatalogResult cr = extractCatalog(request.databaseName());
        if (cr != null) {
            request = request.toBuilder().databaseName(cr.databaseName).catalogId(cr.catalogId).build();
        }
        return super.getUserDefinedFunction(request);
    }

    @Override
    public GetUserDefinedFunctionsResponse getUserDefinedFunctions(GetUserDefinedFunctionsRequest request) {
        CatalogResult cr = extractCatalog(request.databaseName());
        if (cr != null) {
            request = request.toBuilder().databaseName(cr.databaseName).catalogId(cr.catalogId).build();
        }
        return super.getUserDefinedFunctions(request);
    }

    @Override
    public UpdateDatabaseResponse updateDatabase(UpdateDatabaseRequest request) {
        CatalogResult cr = extractCatalog(request.name());
        if (cr != null) {
            request = request.toBuilder().name(cr.databaseName).catalogId(cr.catalogId).build();
        }
        if (request.databaseInput() != null) {
            CatalogResult crInput = extractCatalog(request.databaseInput().name());
            if (crInput != null) {
                request = request.toBuilder()
                    .databaseInput(request.databaseInput().toBuilder().name(crInput.databaseName).build())
                    .build();
            }
        }
        return super.updateDatabase(request);
    }

    @Override
    public UpdatePartitionResponse updatePartition(UpdatePartitionRequest request) {
        CatalogResult cr = extractCatalog(request.databaseName());
        if (cr != null) {
            request = request.toBuilder().databaseName(cr.databaseName).catalogId(cr.catalogId).build();
        }
        return super.updatePartition(request);
    }

    @Override
    public UpdateTableResponse updateTable(UpdateTableRequest request) {
        CatalogResult cr = extractCatalog(request.databaseName());
        if (cr != null) {
            request = request.toBuilder().databaseName(cr.databaseName).catalogId(cr.catalogId).build();
        }
        return super.updateTable(request);
    }

    @Override
    public UpdateUserDefinedFunctionResponse updateUserDefinedFunction(UpdateUserDefinedFunctionRequest request) {
        CatalogResult cr = extractCatalog(request.databaseName());
        if (cr != null) {
            request = request.toBuilder().databaseName(cr.databaseName).catalogId(cr.catalogId).build();
        }
        return super.updateUserDefinedFunction(request);
    }
}
