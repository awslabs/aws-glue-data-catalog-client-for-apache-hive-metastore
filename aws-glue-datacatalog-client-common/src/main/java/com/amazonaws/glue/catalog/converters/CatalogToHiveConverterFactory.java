package com.amazonaws.glue.catalog.converters;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hive.common.util.HiveVersionInfo;

public class CatalogToHiveConverterFactory {

  private static final String HIVE_3_VERSION = "3.";

  private static CatalogToHiveConverter catalogToHiveConverter;

  public static CatalogToHiveConverter getCatalogToHiveConverter() {
    if (catalogToHiveConverter == null) {
      catalogToHiveConverter = loadConverter();
    }
    return catalogToHiveConverter;
  }

  private static CatalogToHiveConverter loadConverter() {
    String hiveVersion = HiveVersionInfo.getShortVersion();

    if (hiveVersion.startsWith(HIVE_3_VERSION)) {
      return new Hive3CatalogToHiveConverter();
    } else {
      return new BaseCatalogToHiveConverter();
    }
  }

  @VisibleForTesting
  static void clearConverter() {
    catalogToHiveConverter = null;
  }
}
