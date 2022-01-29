package com.amazonaws.glue.catalog.converters;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.glue.catalog.exceptions.InvalidPartitionNameException;
import com.google.common.collect.ImmutableSet;

public class PartitionNameParser {

  private static final Pattern PARTITION_NAME_VALUE_PATTERN = Pattern.compile("([^/]+)=([^/]+)");
  private static final String PARTITION_NAME_DELIMITER = "/";

  private static final char STORE_AS_NUMBER = 'n';
  private static final char STORE_AS_STRING = 's';

  private static final Set<String> NUMERIC_PARTITION_COLUMN_TYPES = ImmutableSet.of(
      "tinyint",
      "smallint",
      "int",
      "bigint"
  );

  public static String getPartitionName(List<String> partitionColumns, List<String> partitionValues) {
    if (hasInvalidValues(partitionColumns, partitionValues) || hasInvalidSize(partitionColumns, partitionValues)) {
      throw new IllegalArgumentException("Partition is not well formed. Columns and values do no match.");
    }

    StringBuilder partitionName = new StringBuilder();
    partitionName.append(getPartitionColumnName(partitionColumns.get(0), partitionValues.get(0)));

    for (int i = 1; i < partitionColumns.size(); i++) {
      partitionName.append(PARTITION_NAME_DELIMITER);
      partitionName.append(getPartitionColumnName(partitionColumns.get(i), partitionValues.get(i)));
    }

    return partitionName.toString();
  }

  private static boolean hasInvalidValues(List<String> partitionColumns, List<String> partitionValues) {
    return partitionColumns == null || partitionValues == null;
  }

  private static boolean hasInvalidSize(List<String> partitionColumns, List<String> partitionValues) {
    return partitionColumns.size() != partitionValues.size();
  }

  private static String getPartitionColumnName(String partitionColumn, String partitionValue) {
    return partitionColumn + "=" + partitionValue;
  }

  public static LinkedHashMap<String, String> getPartitionColumns(String partitionName) {
    LinkedHashMap<String, String> partitionColumns = new LinkedHashMap<>();
    String[] partitions = partitionName.split(PARTITION_NAME_DELIMITER);
    for(String partition : partitions) {
      Entry<String, String> entry = getPartitionColumnValuePair(partition);
      partitionColumns.put(entry.getKey(), entry.getValue());
    }

    return partitionColumns;
  }

  /*
   * Copied from https://github.com/apache/hive/blob/master/common/src/java/org/apache/hadoop/hive/common/FileUtils.java
   */
  public static String unescapePathName(String path) {
    int len = path.length();
    //pre-allocate sb to have enough buffer size, to avoid realloc
    StringBuilder sb = new StringBuilder(len);
    for (int i = 0; i < len; i++) {
      char c = path.charAt(i);
      if (c == '%' && i + 2 < len) {
        int code = -1;
        try {
          code = Integer.parseInt(path.substring(i + 1, i + 3), 16);
        } catch (Exception e) {
          code = -1;
        }
        if (code >= 0) {
          sb.append((char) code);
          i += 2;
          continue;
        }
      }
      sb.append(c);
    }
    return sb.toString();
  }

  private static AbstractMap.SimpleEntry getPartitionColumnValuePair(String partition) {
    String column = null;
    String value = null;
    Matcher partitionMatcher = PARTITION_NAME_VALUE_PATTERN.matcher(partition);

    if (partitionMatcher.matches()) {
      column = unescapePathName(partitionMatcher.group(1));
      value = unescapePathName(partitionMatcher.group(2));
    } else {
      throw new InvalidPartitionNameException(partition);
    }

    return new AbstractMap.SimpleEntry(column, value);
  }

  public static List<String> getPartitionValuesFromName(String partitionName) {
    List<String> partitionValues = new ArrayList<>();
    String[] partitions = partitionName.split(PARTITION_NAME_DELIMITER);
    for(String partition : partitions) {
      Entry<String, String> entry = getPartitionColumnValuePair(partition);
      partitionValues.add(entry.getValue());
    }

    return partitionValues;
  }

}
