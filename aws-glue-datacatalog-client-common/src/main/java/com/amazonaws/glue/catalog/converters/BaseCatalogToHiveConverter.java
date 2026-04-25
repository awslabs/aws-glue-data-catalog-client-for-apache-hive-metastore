package com.amazonaws.glue.catalog.converters;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.glue.model.BinaryColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.BooleanColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.ColumnStatistics;
import software.amazon.awssdk.services.glue.model.ColumnStatisticsType;
import software.amazon.awssdk.services.glue.model.DateColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.DecimalColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.DoubleColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.ErrorDetail;
import software.amazon.awssdk.services.glue.model.LongColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.StringColumnStatisticsData;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang3.ObjectUtils.firstNonNull;

public class BaseCatalogToHiveConverter implements CatalogToHiveConverter {

  private static final Logger logger = Logger.getLogger(BaseCatalogToHiveConverter.class);

  private static final ImmutableMap<String, HiveException> EXCEPTION_MAP = ImmutableMap.<String, HiveException>builder()
      .put("AlreadyExistsException", new HiveException() {
        public TException get(String msg) { return new AlreadyExistsException(msg); }
      })
      .put("InvalidInputException", new HiveException() {
        public TException get(String msg) { return new InvalidObjectException(msg); }
      })
      .put("InternalServiceException", new HiveException() {
        public TException get(String msg) { return new MetaException(msg); }
      })
      .put("ResourceNumberLimitExceededException", new HiveException() {
        public TException get(String msg) { return new MetaException(msg); }
      })
      .put("OperationTimeoutException", new HiveException() {
        public TException get(String msg) { return new MetaException(msg); }
      })
      .put("EntityNotFoundException", new HiveException() {
        public TException get(String msg) { return new NoSuchObjectException(msg); }
      })
      .build();

  interface HiveException {
    TException get(String msg);
  }

  public TException wrapInHiveException(Throwable e) {
    if (e instanceof AwsServiceException && ((AwsServiceException) e).awsErrorDetails() != null) {
      return getHiveException(e.getClass().getSimpleName(),
          e.getMessage() + "Error Code: " + ((AwsServiceException) e).awsErrorDetails().errorCode());
    }
    return getHiveException(e.getClass().getSimpleName(), e.getMessage());
  }

  public TException errorDetailToHiveException(ErrorDetail errorDetail) {
    return getHiveException(errorDetail.errorCode(), errorDetail.errorMessage());
  }

  private TException getHiveException(String errorName, String errorMsg) {
    if (EXCEPTION_MAP.containsKey(errorName)) {
      return EXCEPTION_MAP.get(errorName).get(errorMsg);
    } else {
      logger.warn("Hive Exception type not found for " + errorName);
      return new MetaException(errorMsg);
    }
  }

  public Catalog convertCatalog(software.amazon.awssdk.services.glue.model.Catalog glueCatalog) {
    Catalog hiveCatalog = new Catalog();
    hiveCatalog.setName(glueCatalog.name());
    hiveCatalog.setDescription(glueCatalog.description());
    return hiveCatalog;
  }

  public Database convertDatabase(software.amazon.awssdk.services.glue.model.Database catalogDatabase) {
    Database hiveDatabase = new Database();
    hiveDatabase.setName(catalogDatabase.name());
    hiveDatabase.setDescription(catalogDatabase.description());
    String location = catalogDatabase.locationUri();
    hiveDatabase.setLocationUri(location == null ? "" : location);
    hiveDatabase.setParameters(firstNonNull(catalogDatabase.parameters(), Maps.<String, String>newHashMap()));
    return hiveDatabase;
  }

  public FieldSchema convertFieldSchema(software.amazon.awssdk.services.glue.model.Column catalogFieldSchema) {
    FieldSchema hiveFieldSchema = new FieldSchema();
    hiveFieldSchema.setType(catalogFieldSchema.type());
    hiveFieldSchema.setName(catalogFieldSchema.name());
    hiveFieldSchema.setComment(catalogFieldSchema.comment());
    return hiveFieldSchema;
  }

  public List<FieldSchema> convertFieldSchemaList(List<software.amazon.awssdk.services.glue.model.Column> catalogFieldSchemaList) {
    List<FieldSchema> hiveFieldSchemaList = new ArrayList<>();
    if (catalogFieldSchemaList == null) {
      return hiveFieldSchemaList;
    }
    for (software.amazon.awssdk.services.glue.model.Column catalogFieldSchema : catalogFieldSchemaList){
      hiveFieldSchemaList.add(convertFieldSchema(catalogFieldSchema));
    }
    return hiveFieldSchemaList;
  }

  public Table convertTable(software.amazon.awssdk.services.glue.model.Table catalogTable, String dbname) {
    Table hiveTable = new Table();
    hiveTable.setDbName(dbname);
    hiveTable.setTableName(catalogTable.name());
    Instant createTime = catalogTable.createTime();
    hiveTable.setCreateTime(createTime == null ? 0 : (int) (createTime.toEpochMilli() / 1000));
    hiveTable.setOwner(catalogTable.owner());
    Instant lastAccessedTime = catalogTable.lastAccessTime();
    hiveTable.setLastAccessTime(lastAccessedTime == null ? 0 : (int) (lastAccessedTime.toEpochMilli() / 1000));
    hiveTable.setRetention(catalogTable.retention());
    hiveTable.setSd(convertStorageDescriptor(catalogTable.storageDescriptor()));
    hiveTable.setPartitionKeys(convertFieldSchemaList(catalogTable.partitionKeys()));
    Map<String, String> parameterMap = catalogTable.parameters();
    if (parameterMap == null) {
      parameterMap = Maps.newHashMap();
    }
    hiveTable.setParameters(parameterMap);
    hiveTable.setViewOriginalText(catalogTable.viewOriginalText());
    hiveTable.setViewExpandedText(catalogTable.viewExpandedText());
    hiveTable.setTableType(catalogTable.tableType());
    return hiveTable;
  }

  public TableMeta convertTableMeta(software.amazon.awssdk.services.glue.model.Table catalogTable, String dbName) {
    TableMeta tableMeta = new TableMeta();
    tableMeta.setDbName(dbName);
    tableMeta.setTableName(catalogTable.name());
    tableMeta.setTableType(catalogTable.tableType());
    if (catalogTable.parameters().containsKey("comment")) {
      tableMeta.setComments(catalogTable.parameters().get("comment"));
    }
    return tableMeta;
  }

  public StorageDescriptor convertStorageDescriptor(software.amazon.awssdk.services.glue.model.StorageDescriptor catalogSd) {
    StorageDescriptor hiveSd = new StorageDescriptor();
    hiveSd.setCols(convertFieldSchemaList(catalogSd.columns()));
    hiveSd.setLocation(catalogSd.location());
    hiveSd.setInputFormat(catalogSd.inputFormat());
    hiveSd.setOutputFormat(catalogSd.outputFormat());
    hiveSd.setCompressed(catalogSd.compressed());
    hiveSd.setNumBuckets(catalogSd.numberOfBuckets());
    hiveSd.setSerdeInfo(convertSerDeInfo(catalogSd.serdeInfo()));
    hiveSd.setBucketCols(firstNonNull(catalogSd.bucketColumns(), Lists.<String>newArrayList()));
    hiveSd.setSortCols(convertOrderList(catalogSd.sortColumns()));
    hiveSd.setParameters(firstNonNull(catalogSd.parameters(), Maps.<String, String>newHashMap()));
    hiveSd.setSkewedInfo(convertSkewedInfo(catalogSd.skewedInfo()));
    hiveSd.setStoredAsSubDirectories(catalogSd.storedAsSubDirectories());
    return hiveSd;
  }

  public Order convertOrder(software.amazon.awssdk.services.glue.model.Order catalogOrder) {
    Order hiveOrder = new Order();
    hiveOrder.setCol(catalogOrder.column());
    hiveOrder.setOrder(catalogOrder.sortOrder());
    return hiveOrder;
  }

  public List<Order> convertOrderList(List<software.amazon.awssdk.services.glue.model.Order> catalogOrderList) {
    List<Order> hiveOrderList = new ArrayList<>();
    if (catalogOrderList == null) {
      return hiveOrderList;
    }
    for (software.amazon.awssdk.services.glue.model.Order catalogOrder : catalogOrderList){
      hiveOrderList.add(convertOrder(catalogOrder));
    }
    return hiveOrderList;
  }

  public SerDeInfo convertSerDeInfo(software.amazon.awssdk.services.glue.model.SerDeInfo catalogSerDeInfo){
    SerDeInfo hiveSerDeInfo = new SerDeInfo();
    hiveSerDeInfo.setName(catalogSerDeInfo.name());
    hiveSerDeInfo.setParameters(firstNonNull(catalogSerDeInfo.parameters(), Maps.<String, String>newHashMap()));
    hiveSerDeInfo.setSerializationLib(catalogSerDeInfo.serializationLibrary());
    return hiveSerDeInfo;
  }

  public SkewedInfo convertSkewedInfo(software.amazon.awssdk.services.glue.model.SkewedInfo catalogSkewedInfo) {
    if (catalogSkewedInfo == null) {
      return null;
    }
    SkewedInfo hiveSkewedInfo = new SkewedInfo();
    hiveSkewedInfo.setSkewedColNames(firstNonNull(catalogSkewedInfo.skewedColumnNames(), Lists.<String>newArrayList()));
    hiveSkewedInfo.setSkewedColValues(convertSkewedValue(catalogSkewedInfo.skewedColumnValues()));
    hiveSkewedInfo.setSkewedColValueLocationMaps(convertSkewedMap(catalogSkewedInfo.skewedColumnValueLocationMaps()));
    return hiveSkewedInfo;
  }

  public Partition convertPartition(software.amazon.awssdk.services.glue.model.Partition src) {
    Partition tgt = new Partition();
    Instant createTime = src.creationTime();
    if (createTime != null) {
      tgt.setCreateTime((int) (createTime.toEpochMilli() / 1000));
      tgt.setCreateTimeIsSet(true);
    } else {
      tgt.setCreateTimeIsSet(false);
    }
    String dbName = src.databaseName();
    if (dbName != null) {
      tgt.setDbName(dbName);
      tgt.setDbNameIsSet(true);
    } else {
      tgt.setDbNameIsSet(false);
    }
    Instant lastAccessTime = src.lastAccessTime();
    if (lastAccessTime != null) {
      tgt.setLastAccessTime((int) (lastAccessTime.toEpochMilli() / 1000));
      tgt.setLastAccessTimeIsSet(true);
    } else {
      tgt.setLastAccessTimeIsSet(false);
    }
    Map<String, String> params = src.parameters();
    if (params == null) {
      params = Maps.newHashMap();
    }
    tgt.setParameters(params);
    tgt.setParametersIsSet(true);

    String tableName = src.tableName();
    if (tableName != null) {
      tgt.setTableName(tableName);
      tgt.setTableNameIsSet(true);
    } else {
      tgt.setTableNameIsSet(false);
    }

    List<String> values = src.values();
    if (values != null) {
      tgt.setValues(values);
      tgt.setValuesIsSet(true);
    } else {
      tgt.setValuesIsSet(false);
    }

    software.amazon.awssdk.services.glue.model.StorageDescriptor sd = src.storageDescriptor();
    if (sd != null) {
      StorageDescriptor hiveSd = convertStorageDescriptor(sd);
      tgt.setSd(hiveSd);
      tgt.setSdIsSet(true);
    } else {
      tgt.setSdIsSet(false);
    }

    return tgt;
  }

  public List<Partition> convertPartitions(List<software.amazon.awssdk.services.glue.model.Partition> src) {
    if (src == null) {
      return null;
    }
    List<Partition> target = Lists.newArrayList();
    for (software.amazon.awssdk.services.glue.model.Partition partition : src) {
      target.add(convertPartition(partition));
    }
    return target;
  }

  public List<String> convertStringToList(final String s) {
    if (s == null) {
      return null;
    }
    List<String> listString = new ArrayList<>();
    for (int i = 0; i < s.length();) {
      StringBuilder length = new StringBuilder();
      for (int j = i; j < s.length(); j++){
        if (s.charAt(j) != '$') {
          length.append(s.charAt(j));
        } else {
          int lengthOfString = Integer.valueOf(length.toString());
          listString.add(s.substring(j + 1, j + 1 + lengthOfString));
          i = j + 1 + lengthOfString;
          break;
        }
      }
    }
    return listString;
  }

  @Nonnull
  public Map<List<String>, String> convertSkewedMap(final @Nullable Map<String, String> catalogSkewedMap) {
    Map<List<String>, String> skewedMap = new HashMap<>();
    if (catalogSkewedMap == null){
      return skewedMap;
    }
    for (String coralKey : catalogSkewedMap.keySet()) {
      skewedMap.put(convertStringToList(coralKey), catalogSkewedMap.get(coralKey));
    }
    return skewedMap;
  }

  @Nonnull
  public List<List<String>> convertSkewedValue(final @Nullable List<String> catalogSkewedValue) {
    List<List<String>> skewedValues = new ArrayList<>();
    if (catalogSkewedValue == null){
      return skewedValues;
    }
    for (String skewValue : catalogSkewedValue) {
      skewedValues.add(convertStringToList(skewValue));
    }
    return skewedValues;
  }

  public PrincipalType convertPrincipalType(software.amazon.awssdk.services.glue.model.PrincipalType catalogPrincipalType) {
    if(catalogPrincipalType == null) {
      return null;
    }

    if(catalogPrincipalType == software.amazon.awssdk.services.glue.model.PrincipalType.GROUP) {
      return PrincipalType.GROUP;
    } else if(catalogPrincipalType == software.amazon.awssdk.services.glue.model.PrincipalType.USER) {
      return PrincipalType.USER;
    } else if(catalogPrincipalType == software.amazon.awssdk.services.glue.model.PrincipalType.ROLE) {
      return PrincipalType.ROLE;
    }
    throw new RuntimeException("Unknown principal type:" + catalogPrincipalType.toString());
  }

  public Function convertFunction(final String dbName,
                                  final software.amazon.awssdk.services.glue.model.UserDefinedFunction catalogFunction) {
    if (catalogFunction ==  null) {
      return null;
    }
    Function hiveFunction = new Function();
    hiveFunction.setClassName(catalogFunction.className());
    if (catalogFunction.createTime() != null) {
      hiveFunction.setCreateTime((int) (catalogFunction.createTime().toEpochMilli() / 1000));
    }
    hiveFunction.setDbName(dbName);
    hiveFunction.setFunctionName(catalogFunction.functionName());
    hiveFunction.setFunctionType(FunctionType.JAVA);
    hiveFunction.setOwnerName(catalogFunction.ownerName());
    hiveFunction.setOwnerType(convertPrincipalType(software.amazon.awssdk.services.glue.model.PrincipalType.fromValue(catalogFunction.ownerTypeAsString())));
    hiveFunction.setResourceUris(convertResourceUriList(catalogFunction.resourceUris()));
    return hiveFunction;
  }

  public List<ResourceUri> convertResourceUriList(
          final List<software.amazon.awssdk.services.glue.model.ResourceUri> catalogResourceUriList) {
    if (catalogResourceUriList == null) {
      return null;
    }
    List<ResourceUri> hiveResourceUriList = new ArrayList<>();
    for (software.amazon.awssdk.services.glue.model.ResourceUri catalogResourceUri : catalogResourceUriList) {
      ResourceUri hiveResourceUri = new ResourceUri();
      hiveResourceUri.setUri(catalogResourceUri.uri());
      if (catalogResourceUri.resourceType() != null) {
        hiveResourceUri.setResourceType(ResourceType.valueOf(catalogResourceUri.resourceTypeAsString()));
      }
      hiveResourceUriList.add(hiveResourceUri);
    }
    return hiveResourceUriList;
  }

  public List<ColumnStatisticsObj> convertColumnStatisticsList(List<ColumnStatistics> catatlogColumnStatisticsList) {
    List<ColumnStatisticsObj> hiveColumnStatisticsList = new ArrayList<>();
    for (ColumnStatistics catalogColumnStatistics : catatlogColumnStatisticsList) {
      ColumnStatisticsObj hiveColumnStatistics = new ColumnStatisticsObj();
      hiveColumnStatistics.setColName(catalogColumnStatistics.columnName());
      hiveColumnStatistics.setColType(catalogColumnStatistics.columnType());
      hiveColumnStatistics.setStatsData(convertColumnStatisticsData(catalogColumnStatistics.statisticsData()));
      hiveColumnStatisticsList.add(hiveColumnStatistics);
    }
    return hiveColumnStatisticsList;
  }

  private ColumnStatisticsData convertColumnStatisticsData(
      software.amazon.awssdk.services.glue.model.ColumnStatisticsData catalogColumnStatisticsData) {
    ColumnStatisticsData hiveColumnStatisticsData = new ColumnStatisticsData();

    ColumnStatisticsType type = ColumnStatisticsType.fromValue(catalogColumnStatisticsData.typeAsString());
    switch (type) {
      case BINARY:
        BinaryColumnStatisticsData catalogBinaryData = catalogColumnStatisticsData.binaryColumnStatisticsData();
        BinaryColumnStatsData hiveBinaryData = new BinaryColumnStatsData();
        hiveBinaryData.setAvgColLen(catalogBinaryData.averageLength());
        hiveBinaryData.setMaxColLen(catalogBinaryData.maximumLength());
        hiveBinaryData.setNumNulls(catalogBinaryData.numberOfNulls());
        hiveColumnStatisticsData.setBinaryStats(hiveBinaryData);
        break;
      case BOOLEAN:
        BooleanColumnStatisticsData catalogBooleanData = catalogColumnStatisticsData.booleanColumnStatisticsData();
        BooleanColumnStatsData hiveBooleanData = new BooleanColumnStatsData();
        hiveBooleanData.setNumFalses(catalogBooleanData.numberOfFalses());
        hiveBooleanData.setNumTrues(catalogBooleanData.numberOfTrues());
        hiveBooleanData.setNumNulls(catalogBooleanData.numberOfNulls());
        hiveColumnStatisticsData.setBooleanStats(hiveBooleanData);
        break;
      case DATE:
        DateColumnStatisticsData catalogDateData = catalogColumnStatisticsData.dateColumnStatisticsData();
        DateColumnStatsData hiveDateData = new DateColumnStatsData();
        hiveDateData.setLowValue(ConverterUtils.dateToHiveDate(catalogDateData.minimumValue()));
        hiveDateData.setHighValue(ConverterUtils.dateToHiveDate(catalogDateData.maximumValue()));
        hiveDateData.setNumDVs(catalogDateData.numberOfDistinctValues());
        hiveDateData.setNumNulls(catalogDateData.numberOfNulls());
        hiveColumnStatisticsData.setDateStats(hiveDateData);
        break;
      case DECIMAL:
        DecimalColumnStatisticsData catalogDecimalData = catalogColumnStatisticsData.decimalColumnStatisticsData();
        DecimalColumnStatsData hiveDecimalData = new DecimalColumnStatsData();
        hiveDecimalData.setLowValue(convertDecimal(catalogDecimalData.minimumValue()));
        hiveDecimalData.setHighValue(convertDecimal(catalogDecimalData.maximumValue()));
        hiveDecimalData.setNumDVs(catalogDecimalData.numberOfDistinctValues());
        hiveDecimalData.setNumNulls(catalogDecimalData.numberOfNulls());
        hiveColumnStatisticsData.setDecimalStats(hiveDecimalData);
        break;
      case DOUBLE:
        DoubleColumnStatisticsData catalogDoubleData = catalogColumnStatisticsData.doubleColumnStatisticsData();
        DoubleColumnStatsData hiveDoubleData = new DoubleColumnStatsData();
        hiveDoubleData.setLowValue(catalogDoubleData.minimumValue());
        hiveDoubleData.setHighValue(catalogDoubleData.maximumValue());
        hiveDoubleData.setNumDVs(catalogDoubleData.numberOfDistinctValues());
        hiveDoubleData.setNumNulls(catalogDoubleData.numberOfNulls());
        hiveColumnStatisticsData.setDoubleStats(hiveDoubleData);
        break;
      case LONG:
        LongColumnStatisticsData catalogLongData = catalogColumnStatisticsData.longColumnStatisticsData();
        LongColumnStatsData hiveLongData = new LongColumnStatsData();
        hiveLongData.setLowValue(catalogLongData.minimumValue());
        hiveLongData.setHighValue(catalogLongData.maximumValue());
        hiveLongData.setNumDVs(catalogLongData.numberOfDistinctValues());
        hiveLongData.setNumNulls(catalogLongData.numberOfNulls());
        hiveColumnStatisticsData.setLongStats(hiveLongData);
        break;
      case STRING:
        StringColumnStatisticsData catalogStringData = catalogColumnStatisticsData.stringColumnStatisticsData();
        StringColumnStatsData hiveStringData = new StringColumnStatsData();
        hiveStringData.setAvgColLen(catalogStringData.averageLength());
        hiveStringData.setMaxColLen(catalogStringData.maximumLength());
        hiveStringData.setNumDVs(catalogStringData.numberOfDistinctValues());
        hiveStringData.setNumNulls(catalogStringData.numberOfNulls());
        hiveColumnStatisticsData.setStringStats(hiveStringData);
        break;
    }
    return hiveColumnStatisticsData;
  }

  private Decimal convertDecimal(software.amazon.awssdk.services.glue.model.DecimalNumber catalogDecimal) {
    Decimal hiveDecimal = new Decimal();
    hiveDecimal.setUnscaled(catalogDecimal.unscaledValue().asByteBuffer());
    hiveDecimal.setScale(catalogDecimal.scale().shortValue());
    return hiveDecimal;
  }

}
