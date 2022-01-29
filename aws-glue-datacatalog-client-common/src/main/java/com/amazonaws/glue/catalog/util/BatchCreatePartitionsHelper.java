package com.amazonaws.glue.catalog.util;

import com.amazonaws.glue.catalog.converters.BaseCatalogToHiveConverter;
import com.amazonaws.glue.catalog.converters.CatalogToHiveConverter;
import com.amazonaws.glue.catalog.converters.CatalogToHiveConverterFactory;
import com.amazonaws.glue.catalog.converters.GlueInputConverter;
import com.amazonaws.services.glue.model.BatchCreatePartitionRequest;
import com.amazonaws.services.glue.model.BatchCreatePartitionResult;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetPartitionRequest;
import com.amazonaws.services.glue.model.GetPartitionResult;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.PartitionError;
import com.amazonaws.services.glue.AWSGlue;
import com.google.common.collect.Lists;

import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.amazonaws.glue.catalog.util.PartitionUtils.isInvalidUserInputException;

public final class BatchCreatePartitionsHelper {

  private static final Logger logger = Logger.getLogger(BatchCreatePartitionsHelper.class);

  private final AWSGlue client;
  private final String databaseName;
  private final String tableName;
  private final List<Partition> partitions;
  private final boolean ifNotExists;
  private Map<PartitionKey, Partition> partitionMap;
  private List<Partition> partitionsFailed;
  private TException firstTException;
  private String catalogId;
  private CatalogToHiveConverter catalogToHiveConverter;

  public BatchCreatePartitionsHelper(AWSGlue client, String databaseName, String tableName, String catalogId,
                                     List<Partition> partitions, boolean ifNotExists) {
    this.client = client;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.catalogId = catalogId;
    this.partitions = partitions;
    this.ifNotExists = ifNotExists;
    catalogToHiveConverter = CatalogToHiveConverterFactory.getCatalogToHiveConverter();
  }

  public BatchCreatePartitionsHelper createPartitions() {
    partitionMap = PartitionUtils.buildPartitionMap(partitions);
    partitionsFailed = Lists.newArrayList();

    BatchCreatePartitionRequest request = new BatchCreatePartitionRequest()
        .withDatabaseName(databaseName)
        .withTableName(tableName)
        .withCatalogId(catalogId)
        .withPartitionInputList(GlueInputConverter.convertToPartitionInputs(partitionMap.values()));
    
    try {
      BatchCreatePartitionResult result = client.batchCreatePartition(request);
      processResult(result);
    } catch (Exception e) {
      logger.error("Exception thrown while creating partitions in DataCatalog: ", e);
      firstTException = catalogToHiveConverter.wrapInHiveException(e);
      if (isInvalidUserInputException(e)) {
        setAllFailed();
      } else {
        checkIfPartitionsCreated();
      }
    }
    return this;
  }

  private void setAllFailed() {
    partitionsFailed = partitions;
    partitionMap.clear();
  }

  private void processResult(BatchCreatePartitionResult result) {
    List<PartitionError> partitionErrors = result.getErrors();
    if (partitionErrors == null || partitionErrors.isEmpty()) {
      return;
    }

    logger.error(String.format("BatchCreatePartitions failed to create %d out of %d partitions. \n",
        partitionErrors.size(), partitionMap.size()));

    for (PartitionError partitionError : partitionErrors) {
      Partition partitionFailed = partitionMap.remove(new PartitionKey(partitionError.getPartitionValues()));

      TException exception = catalogToHiveConverter.errorDetailToHiveException(partitionError.getErrorDetail());
      if (ifNotExists && exception instanceof AlreadyExistsException) {
        // AlreadyExistsException is allowed, so we shouldn't add the partition to partitionsFailed list
        continue;
      }
      logger.error(exception);
      if (firstTException == null) {
        firstTException = exception;
      }
      partitionsFailed.add(partitionFailed);
    }
  }

  private void checkIfPartitionsCreated() {
    for (Partition partition : partitions) {
      if (!partitionExists(partition)) {
        partitionsFailed.add(partition);
        partitionMap.remove(new PartitionKey(partition));
      }
    }
  }

  private boolean partitionExists(Partition partition) {
    GetPartitionRequest request = new GetPartitionRequest()
        .withDatabaseName(partition.getDatabaseName())
        .withTableName(partition.getTableName())
        .withCatalogId(catalogId)
        .withPartitionValues(partition.getValues());
    
    try {
      GetPartitionResult result = client.getPartition(request);
      Partition partitionReturned = result.getPartition();
      return partitionReturned != null; //probably always true here
    } catch (EntityNotFoundException e) {
      // here we assume namespace and table exist. It is assured by calling "isInvalidUserInputException" method above
      return false;
    } catch (Exception e) {
      logger.error(String.format("Get partition request %s failed. ", request.toString()), e);
      // partition status unknown, we assume that the partition was not created
      return false;
    }
  }

  public TException getFirstTException() {
    return firstTException;
  }

  public Collection<Partition> getPartitionsCreated() {
    return partitionMap.values();
  }

  public List<Partition> getPartitionsFailed() {
    return partitionsFailed;
  }

}
