package com.amazonaws.glue.catalog.converters;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import software.amazon.awssdk.services.glue.model.Table;

import com.google.gson.Gson;

public class ConverterUtils {

  private static final Gson gson = new Gson();

  public static String catalogTableToString(final Table table) {
    return gson.toJson(table);
  }

  public static Table stringToCatalogTable(final String input) {
    return gson.fromJson(input, Table.class);
  }

  public static org.apache.hadoop.hive.metastore.api.Date dateToHiveDate(Instant instant) {
    return new org.apache.hadoop.hive.metastore.api.Date(TimeUnit.MILLISECONDS.toDays(instant.toEpochMilli()));
  }

  public static Instant hiveDatetoDate(org.apache.hadoop.hive.metastore.api.Date hiveDate) {
    return Instant.ofEpochMilli(TimeUnit.DAYS.toMillis(hiveDate.getDaysSinceEpoch()));
  }
}
