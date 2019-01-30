package com.amazonaws.glue.shims;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.metastore.Warehouse;

public interface AwsGlueHiveShims {
  
  ExprNodeGenericFuncDesc getDeserializeExpression(byte[] exprBytes);

  byte[] getSerializeExpression(ExprNodeGenericFuncDesc expr);

  Path getDefaultTablePath(Database db, String tableName, Warehouse warehouse)
      throws MetaException;

  boolean validateTableName(String name, Configuration conf);

  boolean requireCalStats(Configuration conf, Partition oldPart, Partition newPart, Table tbl, EnvironmentContext environmentContext);

  boolean updateTableStatsFast(Database db, Table tbl, Warehouse wh, boolean madeDir, boolean forceRecompute, EnvironmentContext environmentContext)
      throws MetaException;
}
