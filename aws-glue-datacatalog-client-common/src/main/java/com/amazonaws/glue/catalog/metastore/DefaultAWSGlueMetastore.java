package com.amazonaws.glue.catalog.metastore;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import com.amazonaws.glue.catalog.converters.PartitionNameParser;
import com.amazonaws.glue.catalog.util.MetastoreClientUtils;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Catalog;
import software.amazon.awssdk.services.glue.model.CatalogInput;
import software.amazon.awssdk.services.glue.model.CreateCatalogRequest;
import software.amazon.awssdk.services.glue.model.DeleteCatalogRequest;
import software.amazon.awssdk.services.glue.model.GetCatalogRequest;
import software.amazon.awssdk.services.glue.model.GetCatalogResponse;
import software.amazon.awssdk.services.glue.model.UpdateCatalogRequest;
import software.amazon.awssdk.services.glue.model.BatchCreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchGetPartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchGetPartitionResponse;
import software.amazon.awssdk.services.glue.model.ColumnStatistics;
import software.amazon.awssdk.services.glue.model.ColumnStatisticsError;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.CreateUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.DeleteColumnStatisticsForPartitionRequest;
import software.amazon.awssdk.services.glue.model.DeleteColumnStatisticsForTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DeletePartitionRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForPartitionResponse;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForTableRequest;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForTableResponse;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetCatalogsRequest;
import software.amazon.awssdk.services.glue.model.GetCatalogsResponse;
import software.amazon.awssdk.services.glue.model.GetPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionsRequest;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionsResponse;
import software.amazon.awssdk.services.glue.model.Partition;
import software.amazon.awssdk.services.glue.model.PartitionError;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.PartitionValueList;
import software.amazon.awssdk.services.glue.model.Segment;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateColumnStatisticsForPartitionRequest;
import software.amazon.awssdk.services.glue.model.UpdateColumnStatisticsForPartitionResponse;
import software.amazon.awssdk.services.glue.model.UpdateColumnStatisticsForTableRequest;
import software.amazon.awssdk.services.glue.model.UpdateColumnStatisticsForTableResponse;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.UpdatePartitionRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.glue.model.UpdateUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.UserDefinedFunction;
import software.amazon.awssdk.services.glue.model.UserDefinedFunctionInput;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class DefaultAWSGlueMetastore implements AWSGlueMetastore {

    public static final int BATCH_GET_PARTITIONS_MAX_REQUEST_SIZE = 1000;
    /**
     * Based on the maxResults parameter at https://docs.aws.amazon.com/glue/latest/webapi/API_GetPartitions.html
     */
    public static final int GET_PARTITIONS_MAX_SIZE = 1000;
    /**
     * Maximum number of Glue Segments. A segment defines a non-overlapping region of a table's partitions,
     * allowing multiple requests to be executed in parallel.
     */
    public static final int DEFAULT_NUM_PARTITION_SEGMENTS = 5;
    /**
     * Currently the upper limit allowed by Glue is 10.
     * https://docs.aws.amazon.com/glue/latest/webapi/API_Segment.html
     */
    public static final int MAX_NUM_PARTITION_SEGMENTS = 10;
    public static final String NUM_PARTITION_SEGMENTS_CONF = "aws.glue.partition.num.segments";
    public static final String CUSTOM_EXECUTOR_FACTORY_CONF = "hive.metastore.executorservice.factory.class";

    /**
     * Based on the ColumnNames parameter at https://docs.aws.amazon.com/glue/latest/webapi/API_GetColumnStatisticsForPartition.html
     */
    public static final int GET_COLUMNS_STAT_MAX_SIZE = 100;
    public static final int UPDATE_COLUMNS_STAT_MAX_SIZE = 25;

    /**
     * To be used with UpdateTable
     */
    public static final String SKIP_AWS_GLUE_ARCHIVE = "skipAWSGlueArchive";

    private static final int NUM_EXECUTOR_THREADS = 5;
    static final String GLUE_METASTORE_DELEGATE_THREADPOOL_NAME_FORMAT = "glue-metastore-delegate-%d";
    private static final ExecutorService GLUE_METASTORE_DELEGATE_THREAD_POOL = Executors.newFixedThreadPool(
            NUM_EXECUTOR_THREADS,
            new ThreadFactoryBuilder()
                    .setNameFormat(GLUE_METASTORE_DELEGATE_THREADPOOL_NAME_FORMAT)
                    .setDaemon(true).build()
    );

    private final Configuration conf;
    private final GlueClient glueClient;
    private final String catalogId;
    private final ExecutorService executorService;
    private final int numPartitionSegments;

    protected ExecutorService getExecutorService(Configuration conf) {
        Class<? extends ExecutorServiceFactory> executorFactoryClass = conf
                .getClass(CUSTOM_EXECUTOR_FACTORY_CONF,
                        DefaultExecutorServiceFactory.class).asSubclass(
                        ExecutorServiceFactory.class);
        ExecutorServiceFactory factory = ReflectionUtils.newInstance(
                executorFactoryClass, conf);
        return factory.getExecutorService(conf);
    }

    public DefaultAWSGlueMetastore(Configuration conf, GlueClient glueClient) {
        checkNotNull(conf, "Hive Config cannot be null");
        checkNotNull(glueClient, "glueClient cannot be null");
        this.numPartitionSegments = conf.getInt(NUM_PARTITION_SEGMENTS_CONF, DEFAULT_NUM_PARTITION_SEGMENTS);
        checkArgument(numPartitionSegments <= MAX_NUM_PARTITION_SEGMENTS,
                String.format("Hive Config [%s] can't exceed %d", NUM_PARTITION_SEGMENTS_CONF, MAX_NUM_PARTITION_SEGMENTS));
        this.conf = conf;
        this.glueClient = glueClient;
        this.catalogId = MetastoreClientUtils.getCatalogId(conf);
        this.executorService = getExecutorService(conf);
    }

    // ======================= Database =======================

    @Override
    public void createDatabase(DatabaseInput databaseInput) {
        CreateDatabaseRequest createDatabaseRequest = CreateDatabaseRequest.builder()
                .databaseInput(databaseInput).catalogId(catalogId).build();
        glueClient.createDatabase(createDatabaseRequest);
    }

    @Override
    public Database getDatabase(String dbName) {
        GetDatabaseRequest getDatabaseRequest = GetDatabaseRequest.builder().catalogId(catalogId).name(dbName).build();
        GetDatabaseResponse result = glueClient.getDatabase(getDatabaseRequest);
        return result.database();
    }

    @Override
    public List<Database> getAllDatabases() {
        List<Database> ret = Lists.newArrayList();
        String nextToken = null;
        do {
            GetDatabasesRequest getDatabasesRequest = GetDatabasesRequest.builder().nextToken(nextToken).catalogId(
                    catalogId).build();
            GetDatabasesResponse result = glueClient.getDatabases(getDatabasesRequest);
            nextToken = result.nextToken();
            ret.addAll(result.databaseList());
        } while (nextToken != null);
        return ret;
    }

    @Override
    public void updateDatabase(String databaseName, DatabaseInput databaseInput) {
        UpdateDatabaseRequest updateDatabaseRequest = UpdateDatabaseRequest.builder().name(databaseName)
                .databaseInput(databaseInput).catalogId(catalogId).build();
        glueClient.updateDatabase(updateDatabaseRequest);
    }

    @Override
    public void deleteDatabase(String dbName) {
        DeleteDatabaseRequest deleteDatabaseRequest = DeleteDatabaseRequest.builder().name(dbName).catalogId(
                catalogId).build();
        glueClient.deleteDatabase(deleteDatabaseRequest);
    }

    // ======================== Table ========================

    @Override
    public void createTable(String dbName, TableInput tableInput) {
        CreateTableRequest createTableRequest = CreateTableRequest.builder().tableInput(tableInput)
                .databaseName(dbName).catalogId(catalogId).build();
        glueClient.createTable(createTableRequest);
    }

    @Override
    public Table getTable(String dbName, String tableName) {
        GetTableRequest getTableRequest = GetTableRequest.builder().databaseName(dbName).name(tableName)
                .catalogId(catalogId).build();
        GetTableResponse result = glueClient.getTable(getTableRequest);
        return result.table();
    }

    @Override
    public List<Table> getTables(String dbname, String tablePattern) {
        List<Table> ret = new ArrayList<>();
        String nextToken = null;
        do {
            GetTablesRequest getTablesRequest = GetTablesRequest.builder().databaseName(dbname)
                    .expression(tablePattern).nextToken(nextToken).catalogId(catalogId).build();
            GetTablesResponse result = glueClient.getTables(getTablesRequest);
            ret.addAll(result.tableList());
            nextToken = result.nextToken();
        } while (nextToken != null);
        return ret;
    }

    @Override
    public void updateTable(String dbName, TableInput tableInput) {
        UpdateTableRequest updateTableRequest = UpdateTableRequest.builder().databaseName(dbName)
                .tableInput(tableInput).catalogId(catalogId).build();
        glueClient.updateTable(updateTableRequest);
    }

    @Override
    public void updateTable(String dbName, TableInput tableInput, EnvironmentContext environmentContext) {
        UpdateTableRequest updateTableRequest = UpdateTableRequest.builder().databaseName(dbName)
                .tableInput(tableInput).catalogId(catalogId).skipArchive(skipArchive(environmentContext)).build();
        glueClient.updateTable(updateTableRequest);
    }

    private boolean skipArchive(EnvironmentContext environmentContext) {
        return environmentContext != null &&
                environmentContext.isSetProperties() &&
                StatsSetupConst.TRUE.equals(environmentContext.getProperties().get(SKIP_AWS_GLUE_ARCHIVE));
    }

    @Override
    public void deleteTable(String dbName, String tableName) {
        DeleteTableRequest deleteTableRequest = DeleteTableRequest.builder().databaseName(dbName).name(tableName)
                .catalogId(catalogId).build();
        glueClient.deleteTable(deleteTableRequest);
    }

    // =========================== Partition ===========================

    @Override
    public Partition getPartition(String dbName, String tableName, List<String> partitionValues) {
        GetPartitionRequest request = GetPartitionRequest.builder()
                .databaseName(dbName)
                .tableName(tableName)
                .partitionValues(partitionValues)
                .catalogId(catalogId)
                .build();
        return glueClient.getPartition(request).partition();
    }

    @Override
    public List<Partition> getPartitionsByNames(String dbName, String tableName,
                                                List<PartitionValueList> partitionsToGet) {

        List<List<PartitionValueList>> batchedPartitionsToGet = Lists.partition(partitionsToGet,
                BATCH_GET_PARTITIONS_MAX_REQUEST_SIZE);
        List<Future<BatchGetPartitionResponse>> batchGetPartitionFutures = Lists.newArrayList();

        for (List<PartitionValueList> batch : batchedPartitionsToGet) {
            final BatchGetPartitionRequest request = BatchGetPartitionRequest.builder()
                    .databaseName(dbName)
                    .tableName(tableName)
                    .partitionsToGet(batch)
                    .catalogId(catalogId)
                    .build();
            batchGetPartitionFutures.add(this.executorService.submit(new Callable<BatchGetPartitionResponse>() {
                @Override
                public BatchGetPartitionResponse call() throws Exception {
                    return glueClient.batchGetPartition(request);
                }
            }));
        }

        List<Partition> result = Lists.newArrayList();
        try {
            for (Future<BatchGetPartitionResponse> future : batchGetPartitionFutures) {
                result.addAll(future.get().partitions());
            }
        } catch (ExecutionException e) {
            Throwables.propagateIfInstanceOf(e.getCause(), AwsServiceException.class);
            Throwables.propagate(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return result;
    }

    @Override
    public List<Partition> getPartitions(String dbName, String tableName, String expression,
                                         long max) throws TException {
        if (max == 0) {
            return Collections.emptyList();
        }
        if (max < 0 || max > GET_PARTITIONS_MAX_SIZE) {
            return getPartitionsParallel(dbName, tableName, expression, max);
        } else {
            // We don't need to get too many partitions, so just do it serially.
            return getCatalogPartitions(dbName, tableName, expression, max, null);
        }
    }

    private List<Partition> getPartitionsParallel(
            final String databaseName,
            final String tableName,
            final String expression,
            final long max) throws TException {
        // Prepare the segments
        List<Segment> segments = Lists.newArrayList();
        for (int i = 0; i < numPartitionSegments; i++) {
            segments.add(Segment.builder()
                    .segmentNumber(i)
                    .totalSegments(numPartitionSegments)
                    .build());
        }
        // Submit Glue API calls in parallel using the thread pool.
        // We could convert this into a parallelStream after upgrading to JDK 8 compiler base.
        List<Future<List<Partition>>> futures = Lists.newArrayList();
        for (final Segment segment : segments) {
            futures.add(this.executorService.submit(new Callable<List<Partition>>() {
                @Override
                public List<Partition> call() throws Exception {
                    return getCatalogPartitions(databaseName, tableName, expression, max, segment);
                }
            }));
        }

        // Get the results
        List<Partition> partitions = Lists.newArrayList();
        try {
            for (Future<List<Partition>> future : futures) {
                List<Partition> segmentPartitions = future.get();
                if (partitions.size() + segmentPartitions.size() >= max && max > 0) {
                    // Extract the required number of partitions from the segment and we're done.
                    long remaining = max - partitions.size();
                    partitions.addAll(segmentPartitions.subList(0, (int) remaining));
                    break;
                } else {
                    partitions.addAll(segmentPartitions);
                }
            }
        } catch (ExecutionException e) {
            Throwables.propagateIfInstanceOf(e.getCause(), AwsServiceException.class);
            Throwables.propagate(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return partitions;
    }


    private List<Partition> getCatalogPartitions(String databaseName, String tableName, String expression,
                                                 long max, Segment segment) {
        List<Partition> partitions = Lists.newArrayList();
        String nextToken = null;
        do {
            GetPartitionsRequest request = GetPartitionsRequest.builder()
                    .databaseName(databaseName)
                    .tableName(tableName)
                    .expression(expression)
                    .nextToken(nextToken)
                    .catalogId(catalogId)
                    .segment(segment)
                    .build();
            GetPartitionsResponse res = glueClient.getPartitions(request);
            List<Partition> list = res.partitions();
            if ((partitions.size() + list.size()) >= max && max > 0) {
                long remaining = max - partitions.size();
                partitions.addAll(list.subList(0, (int) remaining));
                break;
            }
            partitions.addAll(list);
            nextToken = res.nextToken();
        } while (nextToken != null);
        return partitions;
    }

    @Override
    public void updatePartition(String dbName, String tableName, List<String> partitionValues,
                                PartitionInput partitionInput) {
        UpdatePartitionRequest updatePartitionRequest = UpdatePartitionRequest.builder().databaseName(dbName)
                .tableName(tableName).partitionValueList(partitionValues)
                .partitionInput(partitionInput).catalogId(catalogId).build();
        glueClient.updatePartition(updatePartitionRequest);
    }

    @Override
    public void deletePartition(String dbName, String tableName, List<String> partitionValues) {
        DeletePartitionRequest request = DeletePartitionRequest.builder()
                .databaseName(dbName)
                .tableName(tableName)
                .partitionValues(partitionValues)
                .catalogId(catalogId)
                .build();
        glueClient.deletePartition(request);
    }

    @Override
    public List<PartitionError> createPartitions(String dbName, String tableName,
                                                 List<PartitionInput> partitionInputs) {
        BatchCreatePartitionRequest request =
                BatchCreatePartitionRequest.builder().databaseName(dbName)
                        .tableName(tableName).catalogId(catalogId)
                        .partitionInputList(partitionInputs).build();
        return glueClient.batchCreatePartition(request).errors();
    }

    // ====================== User Defined Function ======================

    @Override
    public void createUserDefinedFunction(String dbName, UserDefinedFunctionInput functionInput) {
        CreateUserDefinedFunctionRequest createUserDefinedFunctionRequest = CreateUserDefinedFunctionRequest.builder()
                .databaseName(dbName).functionInput(functionInput).catalogId(catalogId).build();
        glueClient.createUserDefinedFunction(createUserDefinedFunctionRequest);
    }

    @Override
    public UserDefinedFunction getUserDefinedFunction(String dbName, String functionName) {
        GetUserDefinedFunctionRequest getUserDefinedFunctionRequest = GetUserDefinedFunctionRequest.builder()
                .databaseName(dbName).functionName(functionName).catalogId(catalogId).build();
        return glueClient.getUserDefinedFunction(getUserDefinedFunctionRequest).userDefinedFunction();
    }

    @Override
    public List<UserDefinedFunction> getUserDefinedFunctions(String dbName, String pattern) {
        List<UserDefinedFunction> ret = Lists.newArrayList();
        String nextToken = null;
        do {
            GetUserDefinedFunctionsRequest getUserDefinedFunctionsRequest = GetUserDefinedFunctionsRequest.builder()
                    .databaseName(dbName).pattern(pattern).nextToken(nextToken).catalogId(catalogId).build();
            GetUserDefinedFunctionsResponse result = glueClient.getUserDefinedFunctions(getUserDefinedFunctionsRequest);
            nextToken = result.nextToken();
            ret.addAll(result.userDefinedFunctions());
        } while (nextToken != null);
        return ret;
    }

    @Override
    public List<UserDefinedFunction> getUserDefinedFunctions(String pattern) {
        List<UserDefinedFunction> ret = Lists.newArrayList();
        String nextToken = null;
        do {
            GetUserDefinedFunctionsRequest getUserDefinedFunctionsRequest = GetUserDefinedFunctionsRequest.builder()
                    .pattern(pattern).nextToken(nextToken).catalogId(catalogId).build();
            GetUserDefinedFunctionsResponse result = glueClient.getUserDefinedFunctions(getUserDefinedFunctionsRequest);
            nextToken = result.nextToken();
            ret.addAll(result.userDefinedFunctions());
        } while (nextToken != null);
        return ret;
    }

    @Override
    public void deleteUserDefinedFunction(String dbName, String functionName) {
        DeleteUserDefinedFunctionRequest deleteUserDefinedFunctionRequest = DeleteUserDefinedFunctionRequest.builder()
                .databaseName(dbName).functionName(functionName).catalogId(catalogId).build();
        glueClient.deleteUserDefinedFunction(deleteUserDefinedFunctionRequest);
    }

    @Override
    public void updateUserDefinedFunction(String dbName, String functionName, UserDefinedFunctionInput functionInput) {
        UpdateUserDefinedFunctionRequest updateUserDefinedFunctionRequest = UpdateUserDefinedFunctionRequest.builder()
                .databaseName(dbName).functionName(functionName).functionInput(functionInput)
                .catalogId(catalogId).build();
        glueClient.updateUserDefinedFunction(updateUserDefinedFunctionRequest);
    }

    @Override
    public void deletePartitionColumnStatistics(String dbName, String tableName, List<String> partitionValues, String colName) {
        DeleteColumnStatisticsForPartitionRequest request = DeleteColumnStatisticsForPartitionRequest.builder()
                .catalogId(catalogId)
                .databaseName(dbName)
                .tableName(tableName)
                .partitionValues(partitionValues)
                .columnName(colName)
                .build();
        glueClient.deleteColumnStatisticsForPartition(request);
    }

    @Override
    public void deleteTableColumnStatistics(String dbName, String tableName, String colName) {
        DeleteColumnStatisticsForTableRequest request = DeleteColumnStatisticsForTableRequest.builder()
                .catalogId(catalogId)
                .databaseName(dbName)
                .tableName(tableName)
                .columnName(colName)
                .build();
        glueClient.deleteColumnStatisticsForTable(request);
    }

    @Override
    public Map<String, List<ColumnStatistics>> getPartitionColumnStatistics(String dbName, String tableName, List<String> partitionValues, List<String> columnNames) {
        Map<String, List<ColumnStatistics>> partitionStatistics = new HashMap<>();
        List<List<String>> pagedColNames = Lists.partition(columnNames, GET_COLUMNS_STAT_MAX_SIZE);
        List<String> partValues;
        for (String partName : partitionValues) {
            partValues = PartitionNameParser.getPartitionValuesFromName(partName);
            List<Future<GetColumnStatisticsForPartitionResponse>> pagedResult = new ArrayList<>();
            for (List<String> cols : pagedColNames) {
                GetColumnStatisticsForPartitionRequest request = GetColumnStatisticsForPartitionRequest.builder()
                        .catalogId(catalogId)
                        .databaseName(dbName)
                        .tableName(tableName)
                        .partitionValues(partValues)
                        .columnNames(cols)
                        .build();
                pagedResult.add(GLUE_METASTORE_DELEGATE_THREAD_POOL.submit(new Callable<GetColumnStatisticsForPartitionResponse>() {
                    @Override
                    public GetColumnStatisticsForPartitionResponse call() throws Exception {
                        return glueClient.getColumnStatisticsForPartition(request);
                    }
                }));
            }

            List<ColumnStatistics> result = new ArrayList<>();
            for (Future<GetColumnStatisticsForPartitionResponse> page : pagedResult) {
                try {
                    result.addAll(page.get().columnStatisticsList());
                } catch (ExecutionException e) {
                    Throwables.propagateIfInstanceOf(e.getCause(), AwsServiceException.class);
                    Throwables.propagate(e.getCause());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            partitionStatistics.put(partName, result);
        }
        return partitionStatistics;
    }

    @Override
    public List<ColumnStatistics> getTableColumnStatistics(String dbName, String tableName, List<String> colNames) {
        List<List<String>> pagedColNames = Lists.partition(colNames, GET_COLUMNS_STAT_MAX_SIZE);
        List<Future<GetColumnStatisticsForTableResponse>> pagedResult = new ArrayList<>();

        for (List<String> cols : pagedColNames) {
            GetColumnStatisticsForTableRequest request = GetColumnStatisticsForTableRequest.builder()
                    .catalogId(catalogId)
                    .databaseName(dbName)
                    .tableName(tableName)
                    .columnNames(cols)
                    .build();
            pagedResult.add(GLUE_METASTORE_DELEGATE_THREAD_POOL.submit(new Callable<GetColumnStatisticsForTableResponse>() {
                @Override
                public GetColumnStatisticsForTableResponse call() throws Exception {
                    return glueClient.getColumnStatisticsForTable(request);
                }
            }));
        }
        List<ColumnStatistics> results = new ArrayList<>();

        for (Future<GetColumnStatisticsForTableResponse> page : pagedResult) {
            try {
                results.addAll(page.get().columnStatisticsList());
            } catch (ExecutionException e) {
                Throwables.propagateIfInstanceOf(e.getCause(), AwsServiceException.class);
                Throwables.propagate(e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return results;
    }

    @Override
    public List<ColumnStatisticsError> updatePartitionColumnStatistics(
            String dbName,
            String tableName,
            List<String> partitionValues,
            List<ColumnStatistics> columnStatistics) {

        List<List<ColumnStatistics>> statisticsListPaged = Lists.partition(columnStatistics, UPDATE_COLUMNS_STAT_MAX_SIZE);
        List<Future<UpdateColumnStatisticsForPartitionResponse>> pagedResult = new ArrayList<>();
        for (List<ColumnStatistics> statList : statisticsListPaged) {
            UpdateColumnStatisticsForPartitionRequest request = UpdateColumnStatisticsForPartitionRequest.builder()
                    .catalogId(catalogId)
                    .databaseName(dbName)
                    .tableName(tableName)
                    .partitionValues(partitionValues)
                    .columnStatisticsList(statList)
                    .build();
            pagedResult.add(GLUE_METASTORE_DELEGATE_THREAD_POOL.submit(new Callable<UpdateColumnStatisticsForPartitionResponse>() {
                @Override
                public UpdateColumnStatisticsForPartitionResponse call() throws Exception {
                    return glueClient.updateColumnStatisticsForPartition(request);
                }
            }));
        }
        // Waiting for calls to finish. Will fail the call if one of the future task fails
        List<ColumnStatisticsError> columnStatisticsErrors = new ArrayList<>();
        try {
            for (Future<UpdateColumnStatisticsForPartitionResponse> page : pagedResult) {
                Optional.ofNullable(page.get().errors()).ifPresent(error -> columnStatisticsErrors.addAll(error));
            }
        } catch (ExecutionException e) {
            Throwables.propagateIfInstanceOf(e.getCause(), AwsServiceException.class);
            Throwables.propagate(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return columnStatisticsErrors;
    }

    @Override
    public List<ColumnStatisticsError> updateTableColumnStatistics(
            String dbName,
            String tableName,
            List<ColumnStatistics> columnStatistics) {

        List<List<ColumnStatistics>> statisticsListPaged = Lists.partition(columnStatistics, UPDATE_COLUMNS_STAT_MAX_SIZE);
        List<Future<UpdateColumnStatisticsForTableResponse>> pagedResult = new ArrayList<>();
        for (List<ColumnStatistics> statList : statisticsListPaged) {
            UpdateColumnStatisticsForTableRequest request = UpdateColumnStatisticsForTableRequest.builder()
                    .catalogId(catalogId)
                    .databaseName(dbName)
                    .tableName(tableName)
                    .columnStatisticsList(statList)
                    .build();
            pagedResult.add(GLUE_METASTORE_DELEGATE_THREAD_POOL.submit(new Callable<UpdateColumnStatisticsForTableResponse>() {
                @Override
                public UpdateColumnStatisticsForTableResponse call() throws Exception {
                    return glueClient.updateColumnStatisticsForTable(request);
                }
            }));
        }

        // Waiting for calls to finish. Will fail the call if one of the future task fails
        List<ColumnStatisticsError> columnStatisticsErrors = new ArrayList<>();
        try {
            for (Future<UpdateColumnStatisticsForTableResponse> page : pagedResult) {
                Optional.ofNullable(page.get().errors()).ifPresent(error -> columnStatisticsErrors.addAll(error));
            }
        } catch (ExecutionException e) {
            Throwables.propagateIfInstanceOf(e.getCause(), AwsServiceException.class);
            Throwables.propagate(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return columnStatisticsErrors;
    }

    @Override
    public void createCatalog(String catalogName, CatalogInput catalogInput) {
        glueClient.createCatalog(CreateCatalogRequest.builder()
                .name(catalogName).catalogInput(catalogInput).build());
    }

    @Override
    public Catalog getCatalog(String catalogName) {
        GetCatalogResponse response = glueClient.getCatalog(
                GetCatalogRequest.builder().catalogId(catalogName).build());
        return response.catalog();
    }

    @Override
    public void updateCatalog(String catalogName, CatalogInput catalogInput) {
        glueClient.updateCatalog(UpdateCatalogRequest.builder()
                .catalogId(catalogName).catalogInput(catalogInput).build());
    }

    @Override
    public void deleteCatalog(String catalogName) {
        glueClient.deleteCatalog(DeleteCatalogRequest.builder()
                .catalogId(catalogName).build());
    }

    @Override
    public List<String> getCatalogs() {
        List<String> catalogs = new ArrayList<>();
        String nextToken = null;
        do {
            GetCatalogsRequest.Builder requestBuilder = GetCatalogsRequest.builder()
                    .parentCatalogId(catalogId)
                    .hasDatabases(true)
                    .recursive(true);
            if (nextToken != null) {
                requestBuilder.nextToken(nextToken);
            }
            GetCatalogsResponse response = glueClient.getCatalogs(requestBuilder.build());
            for (software.amazon.awssdk.services.glue.model.Catalog cat : response.catalogList()) {
                catalogs.add(cat.name());
            }
            nextToken = response.nextToken();
        } while (nextToken != null);
        return catalogs;
    }
}