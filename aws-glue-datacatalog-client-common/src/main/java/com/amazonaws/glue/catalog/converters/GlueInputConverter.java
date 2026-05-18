package com.amazonaws.glue.catalog.converters;

import software.amazon.awssdk.services.glue.model.CatalogInput;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UserDefinedFunctionInput;

import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * This class provides methods to convert Hive/Catalog objects to Input objects used
 * for Glue API parameters
 */
public final class GlueInputConverter {

  public static CatalogInput convertToCatalogInput(Catalog hiveCatalog) {
    return CatalogInput.builder()
        .description(hiveCatalog.getDescription())
        .build();
  }

  public static DatabaseInput convertToDatabaseInput(Database hiveDatabase) {
    return convertToDatabaseInput(HiveToCatalogConverter.convertDatabase(hiveDatabase));
  }

  public static DatabaseInput convertToDatabaseInput(software.amazon.awssdk.services.glue.model.Database database) {
    return DatabaseInput.builder()
        .name(database.name())
        .description(database.description())
        .locationUri(database.locationUri())
        .parameters(database.parameters())
        .build();
  }

  public static TableInput convertToTableInput(Table hiveTable) {
    return convertToTableInput(HiveToCatalogConverter.convertTable(hiveTable));
  }

  public static TableInput convertToTableInput(software.amazon.awssdk.services.glue.model.Table table) {
    return TableInput.builder()
        .retention(table.retention())
        .partitionKeys(table.partitionKeys())
        .tableType(table.tableType())
        .name(table.name())
        .owner(table.owner())
        .lastAccessTime(table.lastAccessTime())
        .storageDescriptor(table.storageDescriptor())
        .parameters(table.parameters())
        .viewExpandedText(table.viewExpandedText())
        .viewOriginalText(table.viewOriginalText())
        .build();
  }

  public static PartitionInput convertToPartitionInput(Partition src) {
    return convertToPartitionInput(HiveToCatalogConverter.convertPartition(src));
  }

  public static PartitionInput convertToPartitionInput(software.amazon.awssdk.services.glue.model.Partition src) {
    return PartitionInput.builder()
        .lastAccessTime(src.lastAccessTime())
        .parameters(src.parameters())
        .storageDescriptor(src.storageDescriptor())
        .values(src.values())
        .build();
  }

  public static List<PartitionInput> convertToPartitionInputs(Collection<software.amazon.awssdk.services.glue.model.Partition> parts) {
    List<PartitionInput> inputList = new ArrayList<>();
    for (software.amazon.awssdk.services.glue.model.Partition part : parts) {
      inputList.add(convertToPartitionInput(part));
    }
    return inputList;
  }

  public static UserDefinedFunctionInput convertToUserDefinedFunctionInput(Function hiveFunction) {
    UserDefinedFunctionInput.Builder builder = UserDefinedFunctionInput.builder()
        .className(hiveFunction.getClassName())
        .functionName(hiveFunction.getFunctionName())
        .ownerName(hiveFunction.getOwnerName())
        .resourceUris(HiveToCatalogConverter.covertResourceUriList(hiveFunction.getResourceUris()));
    if(hiveFunction.getOwnerType() != null) {
      builder.ownerType(hiveFunction.getOwnerType().name());
    }
    return builder.build();
  }

}
