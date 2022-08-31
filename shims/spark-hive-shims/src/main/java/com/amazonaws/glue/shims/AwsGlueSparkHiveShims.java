package com.amazonaws.glue.shims;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.metastore.Warehouse;

class AwsGlueSparkHiveShims implements AwsGlueHiveShims {

  private static final String SPARK_HIVE_VERSION = "1.2.";

  static boolean supportsVersion(String version) {
    return version.startsWith(SPARK_HIVE_VERSION);
  }

  @Override
  public ExprNodeGenericFuncDesc getDeserializeExpression(byte[] exprBytes) {
    return Utilities.deserializeExpressionFromKryo(exprBytes);
  }

  @Override
  public byte[] getSerializeExpression(ExprNodeGenericFuncDesc expr) {
    return Utilities.serializeExpressionToKryo(expr);
  }

  @Override
  public Path getDefaultTablePath(Database db, String tableName, Warehouse warehouse) throws MetaException {
    return warehouse.getTablePath(db, tableName);
  }

  @Override
  public boolean validateTableName(String name, Configuration conf) {
    return MetaStoreUtils.validateName(name);
  }

  @Override
  public boolean requireCalStats(
      Configuration conf,
      Partition oldPart,
      Partition newPart,
      Table tbl,
      EnvironmentContext environmentContext) {
    return MetaStoreUtils.requireCalStats(conf, oldPart, newPart, tbl);
  }

  @Override
  public boolean updateTableStatsFast(
      Database db,
      Table tbl,
      Warehouse wh,
      boolean madeDir,
      boolean forceRecompute,
      EnvironmentContext environmentContext
  ) throws MetaException {
    return MetaStoreUtils.updateUnpartitionedTableStatsFast(db, tbl, wh, madeDir, forceRecompute);
  }

}
