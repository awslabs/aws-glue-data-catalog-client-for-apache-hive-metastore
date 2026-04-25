package com.amazonaws.glue.catalog.converters;

import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import software.amazon.awssdk.services.glue.model.BinaryColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.BooleanColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.ColumnStatisticsType;
import software.amazon.awssdk.services.glue.model.DateColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.DecimalColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.DoubleColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.LongColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.StringColumnStatisticsData;
import software.amazon.awssdk.core.SdkBytes;

public class HiveToCatalogConverter {

  public static software.amazon.awssdk.services.glue.model.Database convertDatabase(Database hiveDatabase) {
    return software.amazon.awssdk.services.glue.model.Database.builder()
        .name(hiveDatabase.getName())
        .description(hiveDatabase.getDescription())
        .locationUri(hiveDatabase.getLocationUri())
        .parameters(hiveDatabase.getParameters())
        .build();
  }

  public static software.amazon.awssdk.services.glue.model.Table convertTable(Table hiveTable) {
    return software.amazon.awssdk.services.glue.model.Table.builder()
        .retention(hiveTable.getRetention())
        .partitionKeys(convertFieldSchemaList(hiveTable.getPartitionKeys()))
        .tableType(hiveTable.getTableType())
        .name(hiveTable.getTableName())
        .owner(hiveTable.getOwner())
        .createTime(Instant.ofEpochMilli((long) hiveTable.getCreateTime() * 1000))
        .lastAccessTime(Instant.ofEpochMilli((long) hiveTable.getLastAccessTime() * 1000))
        .storageDescriptor(convertStorageDescriptor(hiveTable.getSd()))
        .parameters(hiveTable.getParameters())
        .viewExpandedText(hiveTable.getViewExpandedText())
        .viewOriginalText(hiveTable.getViewOriginalText())
        .build();
  }

  public static software.amazon.awssdk.services.glue.model.StorageDescriptor convertStorageDescriptor(
          StorageDescriptor hiveSd) {
    return software.amazon.awssdk.services.glue.model.StorageDescriptor.builder()
        .numberOfBuckets(hiveSd.getNumBuckets())
        .compressed(hiveSd.isCompressed())
        .parameters(hiveSd.getParameters())
        .bucketColumns(hiveSd.getBucketCols())
        .columns(convertFieldSchemaList(hiveSd.getCols()))
        .inputFormat(hiveSd.getInputFormat())
        .location(hiveSd.getLocation())
        .outputFormat(hiveSd.getOutputFormat())
        .serdeInfo(convertSerDeInfo(hiveSd.getSerdeInfo()))
        .skewedInfo(convertSkewedInfo(hiveSd.getSkewedInfo()))
        .sortColumns(convertOrderList(hiveSd.getSortCols()))
        .storedAsSubDirectories(hiveSd.isStoredAsSubDirectories())
        .build();
  }

  public static software.amazon.awssdk.services.glue.model.Column convertFieldSchema(
          FieldSchema hiveFieldSchema) {
    return software.amazon.awssdk.services.glue.model.Column.builder()
        .comment(hiveFieldSchema.getComment())
        .name(hiveFieldSchema.getName())
        .type(hiveFieldSchema.getType())
        .build();
  }

  public static List<software.amazon.awssdk.services.glue.model.Column> convertFieldSchemaList(
          List<FieldSchema> hiveFieldSchemaList) {
    List<software.amazon.awssdk.services.glue.model.Column> catalogFieldSchemaList = new ArrayList<>();
    for (FieldSchema hiveFs : hiveFieldSchemaList){
      catalogFieldSchemaList.add(convertFieldSchema(hiveFs));
    }
    return catalogFieldSchemaList;
  }

  public static software.amazon.awssdk.services.glue.model.SerDeInfo convertSerDeInfo(
          SerDeInfo hiveSerDeInfo) {
    return software.amazon.awssdk.services.glue.model.SerDeInfo.builder()
        .name(hiveSerDeInfo.getName())
        .parameters(hiveSerDeInfo.getParameters())
        .serializationLibrary(hiveSerDeInfo.getSerializationLib())
        .build();
  }

  public static software.amazon.awssdk.services.glue.model.SkewedInfo convertSkewedInfo(SkewedInfo hiveSkewedInfo) {
    if (hiveSkewedInfo == null)
      return null;
    return software.amazon.awssdk.services.glue.model.SkewedInfo.builder()
        .skewedColumnNames(hiveSkewedInfo.getSkewedColNames())
        .skewedColumnValues(convertSkewedValue(hiveSkewedInfo.getSkewedColValues()))
        .skewedColumnValueLocationMaps(convertSkewedMap(hiveSkewedInfo.getSkewedColValueLocationMaps()))
        .build();
  }

  public static software.amazon.awssdk.services.glue.model.Order convertOrder(Order hiveOrder) {
    return software.amazon.awssdk.services.glue.model.Order.builder()
        .column(hiveOrder.getCol())
        .sortOrder(hiveOrder.getOrder())
        .build();
  }

  public static List<software.amazon.awssdk.services.glue.model.Order> convertOrderList(List<Order> hiveOrderList) {
    if (hiveOrderList == null) {
      return null;
    }
    List<software.amazon.awssdk.services.glue.model.Order> catalogOrderList = new ArrayList<>();
    for (Order hiveOrder : hiveOrderList) {
      catalogOrderList.add(convertOrder(hiveOrder));
    }
    return catalogOrderList;
  }

  public static software.amazon.awssdk.services.glue.model.Partition convertPartition(Partition src) {
    return software.amazon.awssdk.services.glue.model.Partition.builder()
        .databaseName(src.getDbName())
        .tableName(src.getTableName())
        .creationTime(Instant.ofEpochMilli((long) src.getCreateTime() * 1000))
        .lastAccessTime(Instant.ofEpochMilli((long) src.getLastAccessTime() * 1000))
        .parameters(src.getParameters())
        .storageDescriptor(convertStorageDescriptor(src.getSd()))
        .values(src.getValues())
        .build();
  }

  public static String convertListToString(final List<String> list) {
    if (list == null) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < list.size(); i++) {
      String currentString = list.get(i);
      sb.append(currentString.length() + "$" + currentString);
    }
    return sb.toString();
  }

  public static Map<String, String> convertSkewedMap(final Map<List<String>, String> coreSkewedMap){
    if (coreSkewedMap == null){
      return null;
    }
    Map<String, String> catalogSkewedMap = new HashMap<>();
    for (List<String> coreKey : coreSkewedMap.keySet()) {
      catalogSkewedMap.put(convertListToString(coreKey), coreSkewedMap.get(coreKey));
    }
    return catalogSkewedMap;
  }

  public static List<String> convertSkewedValue(final List<List<String>> coreSkewedValue) {
    if (coreSkewedValue == null) {
      return null;
    }
    List<String> catalogSkewedValue = new ArrayList<>();
    for (int i = 0; i < coreSkewedValue.size(); i++) {
      catalogSkewedValue.add(convertListToString(coreSkewedValue.get(i)));
    }
    return catalogSkewedValue;
  }

  public static software.amazon.awssdk.services.glue.model.UserDefinedFunction convertFunction(final Function hiveFunction) {
    if (hiveFunction == null ){
      return null;
    }
    software.amazon.awssdk.services.glue.model.UserDefinedFunction.Builder builder =
        software.amazon.awssdk.services.glue.model.UserDefinedFunction.builder()
            .className(hiveFunction.getClassName())
            .functionName(hiveFunction.getFunctionName())
            .createTime(Instant.ofEpochMilli((long) (hiveFunction.getCreateTime()) * 1000))
            .ownerName(hiveFunction.getOwnerName())
            .resourceUris(covertResourceUriList(hiveFunction.getResourceUris()));
    if(hiveFunction.getOwnerType() != null) {
      builder.ownerType(hiveFunction.getOwnerType().name());
    }
    return builder.build();
  }

  public static List<software.amazon.awssdk.services.glue.model.ResourceUri> covertResourceUriList(
          final List<ResourceUri> hiveResourceUriList) {
    if (hiveResourceUriList == null) {
      return null;
    }
    List<software.amazon.awssdk.services.glue.model.ResourceUri> catalogResourceUriList = new ArrayList<>();
    for (ResourceUri hiveResourceUri : hiveResourceUriList) {
      software.amazon.awssdk.services.glue.model.ResourceUri.Builder builder =
          software.amazon.awssdk.services.glue.model.ResourceUri.builder()
              .uri(hiveResourceUri.getUri());
      if (hiveResourceUri.getResourceType() != null) {
        builder.resourceType(hiveResourceUri.getResourceType().name());
      }
      catalogResourceUriList.add(builder.build());
    }
    return catalogResourceUriList;
  }

  public static List<software.amazon.awssdk.services.glue.model.ColumnStatistics> convertColumnStatisticsObjList(
      ColumnStatistics hiveColumnStatistics) {
    ColumnStatisticsDesc hiveColumnStatisticsDesc = hiveColumnStatistics.getStatsDesc();
    List<ColumnStatisticsObj> hiveColumnStatisticsObjs = hiveColumnStatistics.getStatsObj();

    List<software.amazon.awssdk.services.glue.model.ColumnStatistics> catalogColumnStatisticsList = new ArrayList<>();
    for (ColumnStatisticsObj hiveColumnStatisticsObj : hiveColumnStatisticsObjs) {
      catalogColumnStatisticsList.add(
          software.amazon.awssdk.services.glue.model.ColumnStatistics.builder()
              .columnName(hiveColumnStatisticsObj.getColName())
              .columnType(hiveColumnStatisticsObj.getColType())
              .analyzedTime(Instant.ofEpochMilli(TimeUnit.DAYS.toMillis(hiveColumnStatisticsDesc.getLastAnalyzed())))
              .statisticsData(convertColumnStatisticsData(hiveColumnStatisticsObj.getStatsData()))
              .build());
    }
    return catalogColumnStatisticsList;
  }

  private static software.amazon.awssdk.services.glue.model.ColumnStatisticsData convertColumnStatisticsData(
      ColumnStatisticsData hiveColumnStatisticsData) {
    software.amazon.awssdk.services.glue.model.ColumnStatisticsData.Builder builder =
        software.amazon.awssdk.services.glue.model.ColumnStatisticsData.builder();

    ColumnStatisticsData._Fields setField = hiveColumnStatisticsData.getSetField();
    switch (setField) {
      case BINARY_STATS:
        BinaryColumnStatsData hiveBinaryData = hiveColumnStatisticsData.getBinaryStats();
        builder.type(ColumnStatisticsType.BINARY.toString())
            .binaryColumnStatisticsData(BinaryColumnStatisticsData.builder()
                .numberOfNulls(hiveBinaryData.getNumNulls())
                .maximumLength(hiveBinaryData.getMaxColLen())
                .averageLength(hiveBinaryData.getAvgColLen())
                .build());
        break;
      case BOOLEAN_STATS:
        BooleanColumnStatsData hiveBooleanData = hiveColumnStatisticsData.getBooleanStats();
        builder.type(ColumnStatisticsType.BOOLEAN.toString())
            .booleanColumnStatisticsData(BooleanColumnStatisticsData.builder()
                .numberOfNulls(hiveBooleanData.getNumNulls())
                .numberOfFalses(hiveBooleanData.getNumFalses())
                .numberOfTrues(hiveBooleanData.getNumTrues())
                .build());
        break;
      case DATE_STATS:
        DateColumnStatsData hiveDateData = hiveColumnStatisticsData.getDateStats();
        builder.type(ColumnStatisticsType.DATE.toString())
            .dateColumnStatisticsData(DateColumnStatisticsData.builder()
                .numberOfNulls(hiveDateData.getNumNulls())
                .numberOfDistinctValues(hiveDateData.getNumDVs())
                .maximumValue(ConverterUtils.hiveDatetoDate(hiveDateData.getHighValue()))
                .minimumValue(ConverterUtils.hiveDatetoDate(hiveDateData.getLowValue()))
                .build());
        break;
      case DECIMAL_STATS:
        DecimalColumnStatsData hiveDecimalData = hiveColumnStatisticsData.getDecimalStats();
        builder.type(ColumnStatisticsType.DECIMAL.toString())
            .decimalColumnStatisticsData(DecimalColumnStatisticsData.builder()
                .numberOfNulls(hiveDecimalData.getNumNulls())
                .numberOfDistinctValues(hiveDecimalData.getNumDVs())
                .maximumValue(convertDecimal(hiveDecimalData.getHighValue()))
                .minimumValue(convertDecimal(hiveDecimalData.getLowValue()))
                .build());
        break;
      case DOUBLE_STATS:
        DoubleColumnStatsData hiveDoubleData = hiveColumnStatisticsData.getDoubleStats();
        builder.type(ColumnStatisticsType.DOUBLE.toString())
            .doubleColumnStatisticsData(DoubleColumnStatisticsData.builder()
                .numberOfNulls(hiveDoubleData.getNumNulls())
                .numberOfDistinctValues(hiveDoubleData.getNumDVs())
                .maximumValue(hiveDoubleData.getHighValue())
                .minimumValue(hiveDoubleData.getLowValue())
                .build());
        break;
      case LONG_STATS:
        LongColumnStatsData hiveLongData = hiveColumnStatisticsData.getLongStats();
        builder.type(ColumnStatisticsType.LONG.toString())
            .longColumnStatisticsData(LongColumnStatisticsData.builder()
                .numberOfNulls(hiveLongData.getNumNulls())
                .numberOfDistinctValues(hiveLongData.getNumDVs())
                .maximumValue(hiveLongData.getHighValue())
                .minimumValue(hiveLongData.getLowValue())
                .build());
        break;
      case STRING_STATS:
        StringColumnStatsData hiveStringData = hiveColumnStatisticsData.getStringStats();
        builder.type(ColumnStatisticsType.STRING.toString())
            .stringColumnStatisticsData(StringColumnStatisticsData.builder()
                .numberOfNulls(hiveStringData.getNumNulls())
                .numberOfDistinctValues(hiveStringData.getNumDVs())
                .maximumLength(hiveStringData.getMaxColLen())
                .averageLength(hiveStringData.getAvgColLen())
                .build());
        break;
    }
    return builder.build();
  }

  private static software.amazon.awssdk.services.glue.model.DecimalNumber convertDecimal(Decimal hiveDecimal) {
    return software.amazon.awssdk.services.glue.model.DecimalNumber.builder()
        .unscaledValue(SdkBytes.fromByteBuffer(ByteBuffer.wrap(hiveDecimal.getUnscaled())))
        .scale((int)hiveDecimal.getScale())
        .build();
  }

}
