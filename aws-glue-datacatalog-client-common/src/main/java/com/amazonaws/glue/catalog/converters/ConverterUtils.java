package com.amazonaws.glue.catalog.converters;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.glue.model.Table;

import com.google.gson.Gson;

public class ConverterUtils {

  private static final Gson gson = new Gson();

  public static String catalogTableToString(final Table table) {
    return gson.toJson(table);
  }

  public static Table stringToCatalogTable(final String input) {
    return gson.fromJson(input, Table.class);
  }

  public static org.apache.hadoop.hive.metastore.api.Date dateToHiveDate(Date date) {
    return new org.apache.hadoop.hive.metastore.api.Date(TimeUnit.MILLISECONDS.toDays(date.getTime()));
  }

  public static Date hiveDatetoDate(org.apache.hadoop.hive.metastore.api.Date hiveDate) {
    return new Date(TimeUnit.DAYS.toMillis(hiveDate.getDaysSinceEpoch()));
  }
}
