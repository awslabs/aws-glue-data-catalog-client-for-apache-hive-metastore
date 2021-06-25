package com.amazonaws.glue.catalog.metastore;

import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.TableInput;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.log4j.Logger;

import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_DB_CACHE_ENABLE;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_DB_CACHE_SIZE;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_DB_CACHE_TTL_MINS;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_TABLE_CACHE_ENABLE;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_TABLE_CACHE_SIZE;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_TABLE_CACHE_TTL_MINS;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_DBS_CACHE_ENABLE;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_DBS_CACHE_SIZE;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_DBS_CACHE_TTL_MINS;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class AWSGlueMetastoreCacheDecorator extends AWSGlueMetastoreBaseDecorator {

    private static final Logger logger = Logger.getLogger(AWSGlueMetastoreCacheDecorator.class);

    private final HiveConf conf;

    private final boolean databaseCacheEnabled;

    private final boolean tableCacheEnabled;

    private final boolean databasesCacheEnabled;

    @VisibleForTesting
    protected Cache<String, Database> databaseCache;
    @VisibleForTesting
    protected Cache<TableIdentifier, Table> tableCache;
    @VisibleForTesting
    protected Cache<String, Database> databasesCache;

    public AWSGlueMetastoreCacheDecorator(HiveConf conf, AWSGlueMetastore awsGlueMetastore) {
        super(awsGlueMetastore);

        checkNotNull(conf, "conf can not be null");
        this.conf = conf;

        databaseCacheEnabled = conf.getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false);
        if(databaseCacheEnabled) {
            int dbCacheSize = conf.getInt(AWS_GLUE_DB_CACHE_SIZE, 0);
            int dbCacheTtlMins = conf.getInt(AWS_GLUE_DB_CACHE_TTL_MINS, 0);

            //validate config values for size and ttl
            validateConfigValueIsGreaterThanZero(AWS_GLUE_DB_CACHE_SIZE, dbCacheSize);
            validateConfigValueIsGreaterThanZero(AWS_GLUE_DB_CACHE_TTL_MINS, dbCacheTtlMins);

            //initialize database cache
            databaseCache = CacheBuilder.newBuilder().maximumSize(dbCacheSize)
                    .expireAfterWrite(dbCacheTtlMins, TimeUnit.MINUTES).build();
        } else {
            databaseCache = null;
        }

        databasesCacheEnabled = conf.getBoolean(AWS_GLUE_DBS_CACHE_ENABLE, false);
        if(databasesCacheEnabled) {
            int dbsCacheSize = conf.getInt(AWS_GLUE_DBS_CACHE_SIZE, 0);
            int dbsCacheTtlMins = conf.getInt(AWS_GLUE_DBS_CACHE_TTL_MINS, 0);

            //validate config values for size and ttl
            validateConfigValueIsGreaterThanZero(AWS_GLUE_DBS_CACHE_SIZE, dbsCacheSize);
            validateConfigValueIsGreaterThanZero(AWS_GLUE_DBS_CACHE_TTL_MINS, dbsCacheTtlMins);

            //initialize database cache
            databasesCache = CacheBuilder.newBuilder().maximumSize(dbsCacheSize)
                    .expireAfterWrite(dbsCacheTtlMins, TimeUnit.MINUTES).build();
        } else {
            databasesCache = null;
        }

        tableCacheEnabled = conf.getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false);
        if(tableCacheEnabled) {
            int tableCacheSize = conf.getInt(AWS_GLUE_TABLE_CACHE_SIZE, 0);
            int tableCacheTtlMins = conf.getInt(AWS_GLUE_TABLE_CACHE_TTL_MINS, 0);

            //validate config values for size and ttl
            validateConfigValueIsGreaterThanZero(AWS_GLUE_TABLE_CACHE_SIZE, tableCacheSize);
            validateConfigValueIsGreaterThanZero(AWS_GLUE_TABLE_CACHE_TTL_MINS, tableCacheTtlMins);

            //initialize table cache
            tableCache = CacheBuilder.newBuilder().maximumSize(tableCacheSize)
                    .expireAfterWrite(tableCacheTtlMins, TimeUnit.MINUTES).build();
        } else {
            tableCache = null;
        }

        logger.info("Constructed");
    }

    private void validateConfigValueIsGreaterThanZero(String configName, int value) {
        checkArgument(value > 0, String.format("Invalid value for Hive Config %s. " +
                "Provide a value greater than zero", configName));

    }

    @Override
    public Database getDatabase(String dbName) {
        Database result;
        if(databaseCacheEnabled) {
            Database valueFromCache = databaseCache.getIfPresent(dbName);
            if(valueFromCache != null) {
                logger.info("Cache hit for operation [getDatabase] on key [" + dbName + "]");
                result = valueFromCache;
            } else {
                logger.info("Cache miss for operation [getDatabase] on key [" + dbName + "]");
                result = super.getDatabase(dbName);
                databaseCache.put(dbName, result);
            }
        } else {
            result = super.getDatabase(dbName);
        }
        return result;
    }

    @Override
    public List<Database> getAllDatabases() {
        List<Database> allDatabases;
        ConcurrentMap<String, Database> collection;
        if (databasesCacheEnabled) {
            Long size = databasesCache.size();
            if (size.compareTo(0L) > 0) {
                logger.info("Get all databases from cache");
                collection = databasesCache.asMap();
                allDatabases = new ArrayList<>(collection.values());
            } else {
                logger.info("Cache miss for getAllDatabases");
                allDatabases = super.getAllDatabases();
                for (Database db : allDatabases) {
                    databasesCache.put(db.getName(), db);
                }
            }
        } else {
            allDatabases = super.getAllDatabases();
        }
        return allDatabases;
    }

    @Override
    public void createDatabase(DatabaseInput databaseInput) {
       super.createDatabase(databaseInput);
       if(databasesCacheEnabled){
           databasesCache.invalidateAll();
       }
    }

    @Override
    public void updateDatabase(String dbName, DatabaseInput databaseInput) {
        super.updateDatabase(dbName, databaseInput);
        if(databaseCacheEnabled) {
            purgeDatabaseFromCache(dbName);
        }
        if(databasesCacheEnabled) {
            databasesCache.invalidateAll();
        }
    }

    @Override
    public void deleteDatabase(String dbName) {
        super.deleteDatabase(dbName);
        if(databaseCacheEnabled) {
            purgeDatabaseFromCache(dbName);
        }
        if(databasesCacheEnabled) {
            databasesCache.invalidate(dbName);
        }
    }

    private void purgeDatabaseFromCache(String dbName) {
        databaseCache.invalidate(dbName);
    }

    @Override
    public Table getTable(String dbName, String tableName) {
        Table result;
        if(tableCacheEnabled) {
            TableIdentifier key = new TableIdentifier(dbName, tableName);
            Table valueFromCache = tableCache.getIfPresent(key);
            if(valueFromCache != null) {
                logger.info("Cache hit for operation [getTable] on key [" + key + "]");
                result = valueFromCache;
            } else {
                logger.info("Cache miss for operation [getTable] on key [" + key + "]");
                result = super.getTable(dbName, tableName);
                tableCache.put(key, result);
            }
        } else {
            result = super.getTable(dbName, tableName);
        }
        return result;
    }

    @Override
    public void updateTable(String dbName, TableInput tableInput) {
        super.updateTable(dbName, tableInput);
        if(tableCacheEnabled) {
            purgeTableFromCache(dbName, tableInput.getName());
        }
    }

    @Override
    public void deleteTable(String dbName, String tableName) {
        super.deleteTable(dbName, tableName);
        if(tableCacheEnabled) {
            purgeTableFromCache(dbName, tableName);
        }
    }

    private void purgeTableFromCache(String dbName, String tableName) {
        TableIdentifier key = new TableIdentifier(dbName, tableName);
        tableCache.invalidate(key);
    }


    static class TableIdentifier {
        private final String dbName;
        private final String tableName;

        public TableIdentifier(String dbName, String tableName) {
            this.dbName = dbName;
            this.tableName = tableName;
        }

        public String getDbName() {
            return dbName;
        }

        public String getTableName() {
            return tableName;
        }

        @Override
        public String toString() {
            return "TableIdentifier{" +
                    "dbName='" + dbName + '\'' +
                    ", tableName='" + tableName + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TableIdentifier that = (TableIdentifier) o;
            return Objects.equals(dbName, that.dbName) &&
                    Objects.equals(tableName, that.tableName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dbName, tableName);
        }
    }
}
