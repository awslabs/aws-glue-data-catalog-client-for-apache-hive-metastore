package com.amazonaws.glue.catalog.converters;

import com.amazonaws.services.glue.model.ErrorDetail;
import com.amazonaws.services.glue.model.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.thrift.TException;

import java.util.List;

public interface CatalogToHiveConverter {

  TException wrapInHiveException(Throwable e);

  TException errorDetailToHiveException(ErrorDetail errorDetail);

  Database convertDatabase(com.amazonaws.services.glue.model.Database catalogDatabase);

  List<FieldSchema> convertFieldSchemaList(List<com.amazonaws.services.glue.model.Column> catalogFieldSchemaList);

  Table convertTable(com.amazonaws.services.glue.model.Table catalogTable, String dbname);

  TableMeta convertTableMeta(com.amazonaws.services.glue.model.Table catalogTable, String dbName);

  Partition convertPartition(com.amazonaws.services.glue.model.Partition src);

  List<Partition> convertPartitions(List<com.amazonaws.services.glue.model.Partition> src);

  Function convertFunction(String dbName, com.amazonaws.services.glue.model.UserDefinedFunction catalogFunction);

  List<ColumnStatisticsObj> convertColumnStatisticsList(List<ColumnStatistics> catatlogColumnStatisticsList);
}
