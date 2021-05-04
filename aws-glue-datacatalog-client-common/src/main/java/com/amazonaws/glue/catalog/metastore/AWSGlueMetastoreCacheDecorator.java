package com.amazonaws.glue.catalog.metastore;

import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.TableInput;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_DB_CACHE_ENABLE;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_DB_CACHE_SIZE;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_DB_CACHE_TTL_MINS;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_TABLE_CACHE_ENABLE;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_TABLE_CACHE_SIZE;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_TABLE_CACHE_TTL_MINS;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_PARTITION_CACHE_ENABLE;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_PARTITION_CACHE_SIZE;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_PARTITION_CACHE_TTL_MINS;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class AWSGlueMetastoreCacheDecorator extends AWSGlueMetastoreBaseDecorator {

    private static final Logger logger = Logger.getLogger(AWSGlueMetastoreCacheDecorator.class);

    private final HiveConf conf;

    private final boolean databaseCacheEnabled;

    private final boolean tableCacheEnabled;
    
    private final boolean partitionCacheEnabled;

    @VisibleForTesting
    protected Cache<String, Database> databaseCache;
    @VisibleForTesting
    protected Cache<TableIdentifier, Table> tableCache;
    @VisibleForTesting
    protected Cache<PartitionIdentifier, Partition> partitionCache; 
    @VisibleForTesting
    protected Cache<PartitionCollectionIdentifier, List<Partition>> partitionCollectionCache;

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
        
        partitionCacheEnabled = conf.getBoolean(AWS_GLUE_PARTITION_CACHE_ENABLE, false);
        if(partitionCacheEnabled) {
             int partitionCacheSize = conf.getInt(AWS_GLUE_PARTITION_CACHE_SIZE, 0);
             int partitionCacheTtlMins = conf.getInt(AWS_GLUE_PARTITION_CACHE_TTL_MINS, 0);
             
             //validate config values for size and ttl
	      validateConfigValueIsGreaterThanZero(AWS_GLUE_PARTITION_CACHE_SIZE, partitionCacheSize);
	      validateConfigValueIsGreaterThanZero(AWS_GLUE_PARTITION_CACHE_TTL_MINS, partitionCacheTtlMins);
	         
	      // initialize partition cache - this cache is used to store one partition of a table
	      partitionCache = CacheBuilder.newBuilder().maximumSize(partitionCacheSize)
	                    .expireAfterWrite(partitionCacheTtlMins, TimeUnit.MINUTES).build();
	         
	      // initialize partition cache - this cache is used to store all partitions of a table
	      partitionCollectionCache = CacheBuilder.newBuilder().maximumSize(partitionCacheSize)
	                    .expireAfterWrite(partitionCacheTtlMins, TimeUnit.MINUTES).build();
             
        } else {
        	partitionCache = null;
        	partitionCollectionCache = null;
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
    public void updateDatabase(String dbName, DatabaseInput databaseInput) {
        super.updateDatabase(dbName, databaseInput);
        if(databaseCacheEnabled) {
            purgeDatabaseFromCache(dbName);
        }
    }

    @Override
    public void deleteDatabase(String dbName) {
        super.deleteDatabase(dbName);
        if(databaseCacheEnabled) {
            purgeDatabaseFromCache(dbName);
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
    
    @Override
    public Partition getPartition(String dbName, String tableName, List<String> partitionValues) {
    	Partition result;
    	if (partitionCacheEnabled) {
    			/**
    			 * Create a key for the partition. It's format is partition_1_key_1-partition_1_key_2-partition_1_key_n
    			 */
    			PartitionIdentifier key = new PartitionIdentifier(dbName, tableName, partitionValues.stream().collect(Collectors.joining("-")));
    			Partition valueFromCache = partitionCache.getIfPresent(key);
    			if (valueFromCache != null) {
    				logger.info("Cache hit for operation [getPartition] on key [" + key + "]");
    				result = valueFromCache;
    			} else {
    				logger.info("Cache miss for operation [getPartition] on key [" + key + "]");
    				result = super.getPartition(dbName, tableName, partitionValues);
    				partitionCache.put(key, result);
    			}
    		} else {
    			result = super.getPartition(dbName, tableName, partitionValues);
    		}
    		return result;
    	}
    
    @Override
	public List<Partition> getPartitions(String dbName, String tableName, String expression, long max)
			throws TException {
		List<Partition> result;
		/**
		 * TODO: For mantis job of Merck, calls to this method do not pass any values for expression and 
		 * max. Partition caching works only when these two parameters come as null values
		 */
		/** DEBUG line 
		
		logger.info(" Partition Cache expression :  [" + expression + "]");
		if (partitionCacheEnabled && !Optional.fromNullable(expression).isPresent() && !Optional.fromNullable(max).isPresent()) {
		*/
		logger.info(" Partition Cache expression :  [" + expression + "] before the IF!");
		if (partitionCacheEnabled) {
			logger.info(" Partition Cache expression :  [" + expression + "] in the IF!");
			PartitionCollectionIdentifier key = new PartitionCollectionIdentifier(dbName, tableName);
			List<Partition> valueFromCache = partitionCollectionCache.getIfPresent(key);
			if (valueFromCache != null) {
				logger.info("Cache hit for operation [getPartitions] on key [" + key + "]");
				result = valueFromCache;
			} else {
				logger.info("Cache miss for operation [getPartitions] on key [" + key + "]");
				result = super.getPartitions(dbName, tableName, expression, max);
				partitionCollectionCache.put(key, result);
			}
		} else {
			result = super.getPartitions(dbName, tableName, expression, max);
		}
		return result;
	}
    
    @Override
	public void updatePartition(String dbName, String tableName, List<String> partitionValues,
			PartitionInput partitionInput) {
		super.updatePartition(dbName, tableName, partitionValues, partitionInput);
		if (partitionCacheEnabled) {
			purgePartitionsFromCache(dbName, tableName, partitionValues);
		}
	}
	
	@Override
	public void deletePartition(String dbName, String tableName, List<String> partitionValues) {
		super.deletePartition(dbName, tableName, partitionValues);
        if(tableCacheEnabled) {
            purgePartitionsFromCache(dbName, tableName, partitionValues);
        }
	}
	
	/**
	 * This method deletes a partition from Cache
	 * @param dbName
	 * @param tableName
	 * @param partitionValues
	 */
	private void purgePartitionsFromCache(String dbName, String tableName, List<String> partitionValues) {
		PartitionIdentifier key = new PartitionIdentifier(dbName, tableName, partitionValues.stream().collect(Collectors.joining("-")));
        partitionCache.invalidate(key);
        
        PartitionCollectionIdentifier pcI = new PartitionCollectionIdentifier(dbName, tableName);
        partitionCollectionCache.invalidate(pcI);
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

    static class PartitionIdentifier {
    	private final String dbName;
    	private final String tableName;
    	private final String partitionName;

    	public PartitionIdentifier(String dbName, String tableName, String partitionName) {
    		this.dbName = dbName;
    		this.tableName = tableName;
    		this.partitionName = partitionName;
    	}

    	public String getDbName() {
    		return dbName;
    	}

    	public String getTableName() {
    		return tableName;
    	}
    	
    	public String getPartitionName() {
    		return partitionName;
    	}

    	@Override
    	public String toString() {
    		return "PartitionIdentifier{" + "dbName='" + dbName + '\'' + ", tableName='" + tableName + '\'' + ", partitionName='" + partitionName + '\'' + '}';
    	}

    	@Override
    	public boolean equals(Object o) {
    		if (this == o)
    			return true;
    		if (o == null || getClass() != o.getClass())
    			return false;
    		PartitionIdentifier that = (PartitionIdentifier) o;
    		return Objects.equals(dbName, that.dbName) && Objects.equals(tableName, that.tableName) && Objects.equals(partitionName, that.partitionName);
    	}

    	@Override
    	public int hashCode() {
    		return Objects.hash(dbName, tableName, partitionName);
    	}
    }

    public class PartitionCollectionIdentifier {

    	private final String dbName;
    	private final String tableName;

    	public PartitionCollectionIdentifier(String dbName, String tableName) {
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
    		return "PartitionCollectionIdentifier{" + "dbName='" + dbName + '\'' + ", tableName='" + tableName + '\'' + '}';
    	}

    	@Override
    	public boolean equals(Object o) {
    		if (this == o)
    			return true;
    		if (o == null || getClass() != o.getClass())
    			return false;
    		PartitionCollectionIdentifier that = (PartitionCollectionIdentifier) o;
    		return Objects.equals(dbName, that.dbName) && Objects.equals(tableName, that.tableName);
    	}

    	@Override
    	public int hashCode() {
    		return Objects.hash(dbName, tableName);
    	}
    }

}
