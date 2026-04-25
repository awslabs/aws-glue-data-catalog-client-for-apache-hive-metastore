package com.amazonaws.glue.catalog.util;

import com.amazonaws.glue.catalog.converters.CatalogToHiveConverter;
import com.amazonaws.glue.catalog.converters.CatalogToHiveConverterFactory;
import software.amazon.awssdk.services.glue.model.BatchDeletePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchDeletePartitionResponse;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.ErrorDetail;
import software.amazon.awssdk.services.glue.model.GetPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionResponse;
import software.amazon.awssdk.services.glue.model.Partition;
import software.amazon.awssdk.services.glue.model.PartitionError;
import software.amazon.awssdk.services.glue.GlueClient;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public final class BatchDeletePartitionsHelper {

  private static final Logger logger = Logger.getLogger(BatchDeletePartitionsHelper.class);

  private final GlueClient client;
  private final String namespaceName;
  private final String tableName;
  private final String catalogId;
  private final List<Partition> partitions;
  private Map<PartitionKey, Partition> partitionMap;
  private TException firstTException;
  private CatalogToHiveConverter catalogToHiveConverter;

  public BatchDeletePartitionsHelper(GlueClient client, String namespaceName, String tableName,
                                     String catalogId, List<Partition> partitions) {
    this.client = client;
    this.namespaceName = namespaceName;
    this.tableName = tableName;
    this.catalogId = catalogId;
    this.partitions = partitions;
    catalogToHiveConverter = CatalogToHiveConverterFactory.getCatalogToHiveConverter();
  }

  public BatchDeletePartitionsHelper deletePartitions() {
    partitionMap = PartitionUtils.buildPartitionMap(partitions);

    BatchDeletePartitionRequest request = BatchDeletePartitionRequest.builder()
        .databaseName(namespaceName)
        .tableName(tableName)
        .catalogId(catalogId)
        .partitionsToDelete(PartitionUtils.getPartitionValuesList(partitionMap))
        .build();

    try {
      BatchDeletePartitionResponse result = client.batchDeletePartition(request);
      processResult(result);
    } catch (Exception e) {
      logger.error("Exception thrown while deleting partitions in DataCatalog: ", e);
      firstTException = catalogToHiveConverter.wrapInHiveException(e);
      if (PartitionUtils.isInvalidUserInputException(e)) {
        setAllFailed();
      } else {
        checkIfPartitionsDeleted();
      }
    }
    return this;
  }

  private void setAllFailed() {
    partitionMap.clear();
  }

  private void processResult(final BatchDeletePartitionResponse batchDeletePartitionsResult) {
    List<PartitionError> partitionErrors = batchDeletePartitionsResult.errors();
    if (partitionErrors == null || partitionErrors.isEmpty()) {
      return;
    }

    logger.error(String.format("BatchDeletePartitions failed to delete %d out of %d partitions. \n",
        partitionErrors.size(), partitionMap.size()));

    for (PartitionError partitionError : partitionErrors) {
      partitionMap.remove(new PartitionKey(partitionError.partitionValues()));
      ErrorDetail errorDetail = partitionError.errorDetail();
      logger.error(errorDetail.toString());
      if (firstTException == null) {
        firstTException = catalogToHiveConverter.errorDetailToHiveException(errorDetail);
      }
    }
  }

  private void checkIfPartitionsDeleted() {
    for (Partition partition : partitions) {
      if (!partitionDeleted(partition)) {
        partitionMap.remove(new PartitionKey(partition));
      }
    }
  }

  private boolean partitionDeleted(Partition partition) {
    GetPartitionRequest request = GetPartitionRequest.builder()
        .databaseName(partition.databaseName())
        .tableName(partition.tableName())
        .partitionValues(partition.values())
        .catalogId(catalogId)
        .build();

    try {
      GetPartitionResponse result = client.getPartition(request);
      Partition partitionReturned = result.partition();
      return partitionReturned == null; //probably always false
    } catch (EntityNotFoundException e) {
      return true;
    } catch (Exception e) {
      logger.error(String.format("Get partition request %s failed. ", request.toString()), e);
      return false;
    }
  }

  public TException getFirstTException() {
    return firstTException;
  }

  public Collection<Partition> getPartitionsDeleted() {
    return partitionMap.values();
  }

}
