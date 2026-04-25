package com.amazonaws.glue.catalog.util;

import software.amazon.awssdk.services.glue.model.Table;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.MetaException;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;

public enum HiveTableValidator {

  REQUIRED_PROPERTIES_VALIDATOR {
    public void validate(Table table) {
      String missingProperty = null;

      if(notApplicableTableType(table)) {
        return;
      }

      if (table.tableType() == null) {
        missingProperty = "TableType";
      } else if (table.storageDescriptor() == null) {
        missingProperty = "StorageDescriptor";
      } else if (table.storageDescriptor().inputFormat() == null) {
        missingProperty = "StorageDescriptor#InputFormat";
      } else if (table.storageDescriptor().outputFormat() == null) {
        missingProperty = "StorageDescriptor#OutputFormat";
      } else if (table.storageDescriptor().serdeInfo() == null) {
        missingProperty = "StorageDescriptor#SerdeInfo";
      } else if (table.storageDescriptor().serdeInfo().serializationLibrary() == null) {
        missingProperty = "StorageDescriptor#SerdeInfo#SerializationLibrary";
      }

      if (missingProperty != null) {
        throw new RuntimeException(new MetaException(
            String.format("%s cannot be null for table: %s", missingProperty, table.name())));
      }
    }
  };

  public abstract void validate(Table table);

  private static boolean notApplicableTableType(Table table) {
    if (isNotManagedOrExternalTable(table) ||
        isStorageHandlerType(table)) {
      return true;
    }
    return false;
  }

  private static boolean isNotManagedOrExternalTable(Table table) {
    if (table.tableType() != null &&
        TableType.valueOf(table.tableType()) != TableType.MANAGED_TABLE &&
        TableType.valueOf(table.tableType()) != TableType.EXTERNAL_TABLE) {
      return true;
    }
    return false;
  }

  private static boolean isStorageHandlerType(Table table) {
    if (table.parameters() != null && table.parameters().containsKey(META_TABLE_STORAGE) &&
        isNotEmpty(table.parameters().get(META_TABLE_STORAGE))) {
      return true;
    }
    return false;
  }
}
