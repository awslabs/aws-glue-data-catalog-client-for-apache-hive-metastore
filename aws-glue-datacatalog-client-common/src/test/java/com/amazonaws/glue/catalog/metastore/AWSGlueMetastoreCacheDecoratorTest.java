package com.amazonaws.glue.catalog.metastore;

import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.TableInput;
import com.google.common.cache.Cache;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_DB_CACHE_ENABLE;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_TABLE_CACHE_ENABLE;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_TABLE_CACHE_SIZE;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_TABLE_CACHE_TTL_MINS;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_DB_CACHE_SIZE;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_DB_CACHE_TTL_MINS;

public class AWSGlueMetastoreCacheDecoratorTest {

    private AWSGlueMetastore glueMetastore;
    private HiveConf hiveConf;

    private static final String DB_NAME = "db";
    private static final String TABLE_NAME = "table";
    private static final AWSGlueMetastoreCacheDecorator.TableIdentifier TABLE_IDENTIFIER =
            new AWSGlueMetastoreCacheDecorator.TableIdentifier(DB_NAME, TABLE_NAME);

    @Before
    public void setUp() {
        glueMetastore = mock(AWSGlueMetastore.class);
        hiveConf = spy(new HiveConf());
        when(hiveConf.getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false)).thenReturn(true);
        when(hiveConf.getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false)).thenReturn(true);
        when(hiveConf.getInt(AWS_GLUE_TABLE_CACHE_SIZE, 0)).thenReturn(100);
        when(hiveConf.getInt(AWS_GLUE_TABLE_CACHE_TTL_MINS, 0)).thenReturn(100);
        when(hiveConf.getInt(AWS_GLUE_DB_CACHE_SIZE, 0)).thenReturn(100);
        when(hiveConf.getInt(AWS_GLUE_DB_CACHE_TTL_MINS, 0)).thenReturn(100);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorWithNullConf() {
        new AWSGlueMetastoreCacheDecorator(null, glueMetastore);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithInvalidTableCacheSize() {
        when(hiveConf.getInt(AWS_GLUE_TABLE_CACHE_SIZE, 0)).thenReturn(0);
        new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithInvalidTableCacheTtl() {
        when(hiveConf.getInt(AWS_GLUE_TABLE_CACHE_TTL_MINS, 0)).thenReturn(0);
        new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithInvalidDbCacheSize() {
        when(hiveConf.getInt(AWS_GLUE_DB_CACHE_SIZE, 0)).thenReturn(0);
        new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithInvalidDbCacheTtl() {
        when(hiveConf.getInt(AWS_GLUE_DB_CACHE_TTL_MINS, 0)).thenReturn(0);
        new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
    }

    @Test
    public void testGetDatabaseWhenCacheDisabled() {
        when(hiveConf.getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false)).thenReturn(false);
        Database db = new Database();
        AWSGlueMetastoreCacheDecorator cacheDecorator =
                new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
        when(glueMetastore.getDatabase(DB_NAME)).thenReturn(db);

        assertEquals(db, cacheDecorator.getDatabase(DB_NAME));
        assertNull(cacheDecorator.databaseCache);
        verify(glueMetastore, times(1)).getDatabase(DB_NAME);
    }

    @Test
    public void testGetDatabaseWhenCacheEnabledAndCacheMiss() {
        Database db = new Database();
        AWSGlueMetastoreCacheDecorator cacheDecorator =
                new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
        assertNotNull(cacheDecorator.databaseCache);
        Cache dbCache = mock(Cache.class);
        cacheDecorator.databaseCache = dbCache;

        when(dbCache.getIfPresent(DB_NAME)).thenReturn(null);
        when(glueMetastore.getDatabase(DB_NAME)).thenReturn(db);
        doNothing().when(dbCache).put(DB_NAME, db);

        assertEquals(db, cacheDecorator.getDatabase(DB_NAME));

        verify(glueMetastore, times(1)).getDatabase(DB_NAME);
        verify(dbCache, times(1)).getIfPresent(DB_NAME);
        verify(dbCache, times(1)).put(DB_NAME, db);
    }

    @Test
    public void testGetDatabaseWhenCacheEnabledAndCacheHit() {
        Database db = new Database();
        AWSGlueMetastoreCacheDecorator cacheDecorator =
                new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
        assertNotNull(cacheDecorator.databaseCache);
        Cache dbCache = mock(Cache.class);
        cacheDecorator.databaseCache = dbCache;

        when(dbCache.getIfPresent(DB_NAME)).thenReturn(db);

        assertEquals(db, cacheDecorator.getDatabase(DB_NAME));

        verify(dbCache, times(1)).getIfPresent(DB_NAME);
    }

    @Test
    public void testCreateDatabase() {
        DatabaseInput databaseInput = new DatabaseInput().withName(DB_NAME);
        AWSGlueMetastoreCacheDecorator cacheDecorator =
                spy(new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore));
        cacheDecorator.createDatabase(databaseInput);

        verify(glueMetastore, times(1)).createDatabase(databaseInput);
        verify(cacheDecorator, times(1)).invalidateDatabaseCache(DB_NAME);
    }

    @Test
    public void testUpdateDatabase() {
        DatabaseInput databaseInput = new DatabaseInput().withName(DB_NAME);
        AWSGlueMetastoreCacheDecorator cacheDecorator =
                spy(new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore));

        cacheDecorator.updateDatabase(DB_NAME, databaseInput);

        verify(glueMetastore, times(1)).updateDatabase(DB_NAME, databaseInput);
        verify(cacheDecorator, times(1)).invalidateDatabaseCache(DB_NAME);
    }

    @Test
    public void testDeleteDatabase() {
        AWSGlueMetastoreCacheDecorator cacheDecorator =
                spy(new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore));

        cacheDecorator.deleteDatabase(DB_NAME);

        verify(glueMetastore, times(1)).deleteDatabase(DB_NAME);
        verify(cacheDecorator, times(1)).invalidateDatabaseCache(DB_NAME);
    }

    @Test
    public void testGetTableWhenCacheDisabled() {
        when(hiveConf.getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false)).thenReturn(false);
        Table table = new Table();
        AWSGlueMetastoreCacheDecorator cacheDecorator =
                new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
        when(glueMetastore.getTable(DB_NAME, TABLE_NAME)).thenReturn(table);

        assertEquals(table, cacheDecorator.getTable(DB_NAME, TABLE_NAME));
        assertNull(cacheDecorator.tableCache);
        verify(glueMetastore, times(1)).getTable(DB_NAME, TABLE_NAME);
    }

    @Test
    public void testGetTableWhenCacheEnabledAndCacheMiss() {
        Table table = new Table();
        AWSGlueMetastoreCacheDecorator cacheDecorator =
                new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
        assertNotNull(cacheDecorator.tableCache);
        Cache tableCache = mock(Cache.class);
        cacheDecorator.tableCache = tableCache;

        when(tableCache.getIfPresent(TABLE_IDENTIFIER)).thenReturn(null);
        when(glueMetastore.getTable(DB_NAME, TABLE_NAME)).thenReturn(table);
        doNothing().when(tableCache).put(TABLE_IDENTIFIER, table);

        assertEquals(table, cacheDecorator.getTable(DB_NAME, TABLE_NAME));

        verify(glueMetastore, times(1)).getTable(DB_NAME, TABLE_NAME);
        verify(tableCache, times(1)).getIfPresent(TABLE_IDENTIFIER);
        verify(tableCache, times(1)).put(TABLE_IDENTIFIER, table);
    }

    @Test
    public void testGetTableWhenCacheEnabledAndCacheHit() {
        Table table = new Table();
        AWSGlueMetastoreCacheDecorator cacheDecorator =
                new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
        assertNotNull(cacheDecorator.tableCache);
        Cache tableCache = mock(Cache.class);
        cacheDecorator.tableCache = tableCache;

        when(tableCache.getIfPresent(TABLE_IDENTIFIER)).thenReturn(table);

        assertEquals(table, cacheDecorator.getTable(DB_NAME, TABLE_NAME));

        verify(tableCache, times(1)).getIfPresent(TABLE_IDENTIFIER);
    }

    @Test
    public void testCreateTable() {
        TableInput tableInput = new TableInput().withName(TABLE_NAME);
        AWSGlueMetastoreCacheDecorator cacheDecorator =
                spy(new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore));

        cacheDecorator.createTable(DB_NAME, tableInput);

        verify(glueMetastore, times(1)).createTable(DB_NAME, tableInput);
        verify(cacheDecorator, times(1)).invalidateTableCache(DB_NAME, TABLE_NAME);
    }

    @Test
    public void testUpdateTable() {
        TableInput tableInput = new TableInput().withName(TABLE_NAME);
        AWSGlueMetastoreCacheDecorator cacheDecorator =
                spy(new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore));

        cacheDecorator.updateTable(DB_NAME, tableInput);

        verify(glueMetastore, times(1)).updateTable(DB_NAME, tableInput);
        verify(cacheDecorator, times(1)).invalidateTableCache(DB_NAME, TABLE_NAME);
    }

    @Test
    public void testDeleteTable() {
        AWSGlueMetastoreCacheDecorator cacheDecorator =
                spy(new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore));

        cacheDecorator.deleteTable(DB_NAME, TABLE_NAME);

        verify(glueMetastore, times(1)).deleteTable(DB_NAME, TABLE_NAME);
        verify(cacheDecorator, times(1)).invalidateTableCache(DB_NAME, TABLE_NAME);
    }
}
