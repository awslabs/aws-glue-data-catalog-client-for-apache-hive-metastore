package com.amazonaws.glue.shims;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hive.common.util.HiveVersionInfo;

public final class ShimsLoader {

  private static AwsGlueHiveShims hiveShims;

  public static synchronized AwsGlueHiveShims getHiveShims() {
    if (hiveShims == null) {
      hiveShims = loadHiveShims();
    }
    return hiveShims;
  }

  private static AwsGlueHiveShims loadHiveShims() {
    String hiveVersion = HiveVersionInfo.getShortVersion();

    try {
      if (AwsGlueSparkHiveShims.supportsVersion(hiveVersion)) {
          return AwsGlueSparkHiveShims.class.newInstance();
      } else if (AwsGlueHive3Shims.supportsVersion(hiveVersion)) {
        return AwsGlueHive3Shims.class.newInstance();
      }
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException("unable to get instance of Hive shim class for " + hiveVersion);
    }


    throw new RuntimeException("Shim class for Hive version " + hiveVersion + " does not exist");
  }

  @VisibleForTesting
  static synchronized void clearShimClass() {
    hiveShims = null;
  }

}
