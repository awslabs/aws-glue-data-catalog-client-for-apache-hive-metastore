package com.amazonaws.glue.shims;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hive.common.util.HiveVersionInfo;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

public final class ShimsLoader {

  // The Spark-on-Hive shim is optional: it depends on a Spark-flavored Hive that is not
  // available in a plain Apache Hive build. Reference it reflectively so this loader (and
  // anything that depends on it, e.g. the hive3 client) can be built and run against
  // released Apache Hive without the spark-hive-shims artifact on the classpath. When the
  // shim IS present (the default/EMR build) the behavior is identical to before.
  private static final String SPARK_HIVE_SHIMS =
      "com.amazonaws.glue.shims.AwsGlueSparkHiveShims";

  private static AwsGlueHiveShims hiveShims;

  public static synchronized AwsGlueHiveShims getHiveShims() {
    if (hiveShims == null) {
      hiveShims = loadHiveShims();
    }
    return hiveShims;
  }

  private static AwsGlueHiveShims loadHiveShims() {
    String hiveVersion = HiveVersionInfo.getShortVersion();

    AwsGlueHiveShims sparkShims = tryLoadSparkHiveShims(hiveVersion);
    if (sparkShims != null) {
      return sparkShims;
    }

    try {
      if (AwsGlueHive3Shims.supportsVersion(hiveVersion)) {
        return AwsGlueHive3Shims.class.newInstance();
      }
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException("unable to get instance of Hive shim class for " + hiveVersion);
    }

    throw new RuntimeException("Shim class for Hive version " + hiveVersion + " does not exist");
  }

  /**
   * Loads the Spark-on-Hive shim reflectively if it is on the classpath and supports the
   * running Hive version. Returns {@code null} when the shim is absent (a plain Apache Hive
   * build), so the caller falls through to the standard Hive 3 shim.
   */
  private static AwsGlueHiveShims tryLoadSparkHiveShims(String hiveVersion) {
    final Class<?> sparkShimsClass;
    try {
      sparkShimsClass = Class.forName(SPARK_HIVE_SHIMS);
    } catch (ClassNotFoundException absent) {
      return null;
    }
    try {
      Method supportsVersion = sparkShimsClass.getDeclaredMethod("supportsVersion", String.class);
      supportsVersion.setAccessible(true);
      if (!((Boolean) supportsVersion.invoke(null, hiveVersion))) {
        return null;
      }
      Constructor<?> ctor = sparkShimsClass.getDeclaredConstructor();
      ctor.setAccessible(true);
      return (AwsGlueHiveShims) ctor.newInstance();
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("unable to get instance of Spark Hive shim class for " + hiveVersion, e);
    }
  }

  @VisibleForTesting
  static synchronized void clearShimClass() {
    hiveShims = null;
  }

}
