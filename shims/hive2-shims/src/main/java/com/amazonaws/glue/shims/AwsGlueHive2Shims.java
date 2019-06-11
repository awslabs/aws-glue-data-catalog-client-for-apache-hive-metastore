package com.amazonaws.glue.shims;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.metastore.Warehouse;

final class AwsGlueHive2Shims implements AwsGlueHiveShims {

  private static final String HIVE_2_VERSION = "1.";

  static boolean supportsVersion(String version) {
    return version.startsWith(HIVE_2_VERSION);
  }

  @Override
  public ExprNodeGenericFuncDesc getDeserializeExpression(byte[] exprBytes) {
    return SerializationUtilities.deserializeExpressionFromKryo(exprBytes);
  }

  @Override
  public byte[] getSerializeExpression(ExprNodeGenericFuncDesc expr) {
    return SerializationUtilities.serializeExpressionToKryo(expr);
  }

  @Override
  public Path getDefaultTablePath(Database db, String tableName, Warehouse warehouse) throws MetaException {
    return warehouse.getDefaultTablePath(db, tableName);
  }

  @Override
  public boolean validateTableName(String name, Configuration conf) {
    return true;
  }

  @Override
  public boolean requireCalStats(
      Configuration conf,
      Partition oldPart,
      Partition newPart,
      Table tbl,
      EnvironmentContext environmentContext) {
    return MetaStoreUtils.requireCalStats(conf, oldPart, newPart, tbl, environmentContext);
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
    return MetaStoreUtils.updateTableStatsFast(db, tbl, wh, madeDir, forceRecompute, environmentContext);
  }

}

