package com.amazonaws.glue.catalog.converters;

import software.amazon.awssdk.services.glue.model.ErrorDetail;
import software.amazon.awssdk.services.glue.model.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.Catalog;
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

  Catalog convertCatalog(software.amazon.awssdk.services.glue.model.Catalog glueCatalog);

  Database convertDatabase(software.amazon.awssdk.services.glue.model.Database catalogDatabase);

  List<FieldSchema> convertFieldSchemaList(List<software.amazon.awssdk.services.glue.model.Column> catalogFieldSchemaList);

  Table convertTable(software.amazon.awssdk.services.glue.model.Table catalogTable, String dbname);

  TableMeta convertTableMeta(software.amazon.awssdk.services.glue.model.Table catalogTable, String dbName);

  Partition convertPartition(software.amazon.awssdk.services.glue.model.Partition src);

  List<Partition> convertPartitions(List<software.amazon.awssdk.services.glue.model.Partition> src);

  Function convertFunction(String dbName, software.amazon.awssdk.services.glue.model.UserDefinedFunction catalogFunction);

  List<ColumnStatisticsObj> convertColumnStatisticsList(List<ColumnStatistics> catatlogColumnStatisticsList);
}
