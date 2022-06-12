package com.amazonaws.glue.shims;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

class AwsGlueSparkHiveShims implements AwsGlueHiveShims {

  private static final Logger logger = Logger.getLogger(AwsGlueSparkHiveShims.class);

  private static final String SPARK_HIVE_VERSION = "1.2.";

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
    return warehouse.getDefaultTablePath(db, tableName);
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
    return updateUnpartitionedTableStatsFast(db, tbl, wh, madeDir, forceRecompute);
  }

  /**
   * Ref: https://github.com/facebookarchive/swift-hive-metastore/blob/9c89d3ce58cfdb3b13e4f5fde8ec28a701ef6885/src/main/java/org/apache/hadoop/hive/metastore/MetaStoreUtils.java#L1295
   * Updates the numFiles and totalSize parameters for the passed unpartitioned Table by querying
   * the warehouse if the passed Table does not already have values for these parameters.
   * @param db
   * @param tbl
   * @param wh
   * @param madeDir if true, the directory was just created and can be assumed to be empty
   * @param forceRecompute Recompute stats even if the passed Table already has
   * these parameters set
   * @return true if the stats were updated, false otherwise
   */
  public static boolean updateUnpartitionedTableStatsFast(
      Database db,
      Table tbl,
      Warehouse wh,
      boolean madeDir,
      boolean forceRecompute
  ) throws MetaException {

    FileStatus[] fileStatus = wh.getFileStatusesForUnpartitionedTable(db, tbl);
    Map<String, String> params = tbl.getParameters();

    if ((params != null) && params.containsKey(StatsSetupConst.DO_NOT_UPDATE_STATS)) {
      boolean doNotUpdateStats = Boolean.parseBoolean(params.get(StatsSetupConst.DO_NOT_UPDATE_STATS));
      params.remove(StatsSetupConst.DO_NOT_UPDATE_STATS);
      tbl.setParameters(params); // to make sure we remove this marker property
      if (doNotUpdateStats) {
        return false;
      }
    }

    boolean updated = false;
    if (forceRecompute || params == null || !MetaStoreUtils.containsAllFastStats(params)) {
      if (params == null) {
        params = new HashMap<>();
      }
      if (!madeDir) {
        // The table location already exists and may contain data.
        // Let's try to populate those stats that don't require full scan.
        logger.info("Updating table stats fast for " + tbl.getTableName());
        MetaStoreUtils.populateQuickStats(fileStatus, params);
        logger.info("Updated size of table " + tbl.getTableName() + " to " + params.get(StatsSetupConst.TOTAL_SIZE));
        if (!params.containsKey(StatsSetupConst.STATS_GENERATED)) {
          // invalidate stats requiring scan since this is a regular ddl alter case
          for (String stat : StatsSetupConst.statsRequireCompute) {
            params.put(stat, "-1");
          }
          params.put(StatsSetupConst.COLUMN_STATS_ACCURATE, StatsSetupConst.FALSE);
        } else {
          params.remove(StatsSetupConst.STATS_GENERATED);
          params.put(StatsSetupConst.COLUMN_STATS_ACCURATE, StatsSetupConst.TRUE);
        }
      }
      tbl.setParameters(params);
      updated = true;
    }
    return updated;
  }
}
