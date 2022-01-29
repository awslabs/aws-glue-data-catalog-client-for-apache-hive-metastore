package com.amazonaws.glue.catalog.converters;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;

public class Hive3CatalogToHiveConverter extends BaseCatalogToHiveConverter {

  @Override
  public Database convertDatabase(com.amazonaws.services.glue.model.Database catalogDatabase) {
    Database hiveDatabase = super.convertDatabase(catalogDatabase);
    hiveDatabase.setCatalogName(DEFAULT_CATALOG_NAME);
    return hiveDatabase;
  }

  @Override
  public Table convertTable(com.amazonaws.services.glue.model.Table catalogTable, String dbname) {
    Table hiveTable = super.convertTable(catalogTable, dbname);
    hiveTable.setCatName(DEFAULT_CATALOG_NAME);
    return hiveTable;
  }

  @Override
  public TableMeta convertTableMeta(com.amazonaws.services.glue.model.Table catalogTable, String dbName) {
    TableMeta tableMeta = super.convertTableMeta(catalogTable, dbName);
    tableMeta.setCatName(DEFAULT_CATALOG_NAME);
    return tableMeta;
  }

  @Override
  public Partition convertPartition(com.amazonaws.services.glue.model.Partition src) {
    Partition hivePartition = super.convertPartition(src);
    hivePartition.setCatName(DEFAULT_CATALOG_NAME);
    return hivePartition;
  }

  @Override
  public Function convertFunction(String dbName, com.amazonaws.services.glue.model.UserDefinedFunction catalogFunction) {
    Function hiveFunction = super.convertFunction(dbName, catalogFunction);
    if (hiveFunction == null) {
      return null;
    }
    hiveFunction.setCatName(DEFAULT_CATALOG_NAME);
    return hiveFunction;
  }
}
