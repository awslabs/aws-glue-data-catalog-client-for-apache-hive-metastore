package com.amazonaws.glue.shims;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;

import java.util.List;

class AwsGlueSparkHiveShims implements AwsGlueHiveShims {

  // Changed from "2.3." to "3." for Hive 3 compatibility
  private static final String SPARK_HIVE_VERSION = "3.";

  static boolean supportsVersion(String version) {
    return version.startsWith(SPARK_HIVE_VERSION);
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
    // Hive 3: added boolean parameter
    return warehouse.getDefaultTablePath(db, tableName, false);
  }

  @Override
  public boolean deleteDir(Warehouse wh, Path path, boolean recursive, boolean ifPurge) throws MetaException {
    // Hive 3: added 4th boolean parameter
    return wh.deleteDir(path, recursive, ifPurge, true);
  }

  @Override
  public boolean mkdirs(Warehouse wh, Path path) throws MetaException {
    // Hive 3: single-arg mkdirs
    return wh.mkdirs(path);
  }

  @Override
  public boolean validateTableName(String name, Configuration conf) {
    return MetaStoreUtils.validateName(name, conf);
  }

  @Override
  public boolean requireCalStats(
      Configuration conf,
      Partition oldPart,
      Partition newPart,
      Table tbl,
      EnvironmentContext environmentContext) {
    // Hive 3: first param changed from conf to oldPart
    return MetaStoreUtils.requireCalStats(oldPart, newPart, tbl, environmentContext);
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
    // Hive 3: renamed from updateTableStatsFast to updateTableStatsSlow, returns void
    MetaStoreUtils.updateTableStatsSlow(db, tbl, wh, madeDir, forceRecompute, environmentContext);
    return true;
  }

  @Override
  public String validateTblColumns(List<FieldSchema> cols) {
    return MetaStoreUtils.validateTblColumns(cols);
  }

}
