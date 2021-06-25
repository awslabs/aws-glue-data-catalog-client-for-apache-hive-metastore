package com.amazonaws.glue.catalog.metastore;

import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.TableInput;
import com.google.common.cache.Cache;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertTrue;

import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_DB_CACHE_ENABLE;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_TABLE_CACHE_ENABLE;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_TABLE_CACHE_SIZE;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_TABLE_CACHE_TTL_MINS;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_DB_CACHE_SIZE;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_DB_CACHE_TTL_MINS;

public class AWSGlueMetastoreCacheDecoratorTest {

    private AWSGlueMetastore glueMetastore;
    private HiveConf hiveConf;

    private static final String DATABASES_CACHE_KEY = "*";
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

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithInvalidDbsCacheSize() {
        when(hiveConf.getInt(AWS_GLUE_DB_CACHE_SIZE, 0)).thenReturn(0);
        new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithInvalidDbsCacheTtl() {
        when(hiveConf.getInt(AWS_GLUE_DB_CACHE_TTL_MINS, 0)).thenReturn(0);
        new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
    }

    @Test
    public void testGetDatabaseWhenCacheDisabled() {
        //disable cache
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
    public void testUpdateDatabaseWhenCacheDisabled() {
        //disable cache
        when(hiveConf.getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false)).thenReturn(false);
        DatabaseInput dbInput = new DatabaseInput();
        AWSGlueMetastoreCacheDecorator cacheDecorator =
                new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
        doNothing().when(glueMetastore).updateDatabase(DB_NAME, dbInput);
        cacheDecorator.updateDatabase(DB_NAME, dbInput);
        assertNull(cacheDecorator.databaseCache);
        assertNull(cacheDecorator.databasesCache);
        verify(glueMetastore, times(1)).updateDatabase(DB_NAME, dbInput);
    }

    @Test
    public void testUpdateDatabaseWhenCacheEnabled() {
        DatabaseInput dbInput = new DatabaseInput();
        AWSGlueMetastoreCacheDecorator cacheDecorator =
                new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
        cacheDecorator.databaseCache.put(DB_NAME, new Database());
        doNothing().when(glueMetastore).updateDatabase(DB_NAME, dbInput);

        cacheDecorator.updateDatabase(DB_NAME, dbInput);

        //db should have been removed from cache
        assertNull(cacheDecorator.databaseCache.getIfPresent(DB_NAME));
        verify(glueMetastore, times(1)).updateDatabase(DB_NAME, dbInput);
        assertEquals(0L, cacheDecorator.databasesCache.size());
    }

    @Test
    public void testDeleteDatabaseWhenCacheDisabled() {
        //disable cache
        when(hiveConf.getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false)).thenReturn(false);
        AWSGlueMetastoreCacheDecorator cacheDecorator =
                new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
        doNothing().when(glueMetastore).deleteDatabase(DB_NAME);
        cacheDecorator.deleteDatabase(DB_NAME);
        assertNull(cacheDecorator.databaseCache);
        assertNull(cacheDecorator.databasesCache);
        verify(glueMetastore, times(1)).deleteDatabase(DB_NAME);
    }

    @Test
    public void testDeleteDatabaseWhenCacheEnabled() {
        DatabaseInput dbInput = new DatabaseInput();
        List<String> names = new ArrayList<>();
        AWSGlueMetastoreCacheDecorator cacheDecorator =
                new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
        cacheDecorator.databaseCache.put(DB_NAME, new Database());
        names.add(DB_NAME);
        cacheDecorator.databasesCache.put(DATABASES_CACHE_KEY, names);
        doNothing().when(glueMetastore).deleteDatabase(DB_NAME);

        cacheDecorator.deleteDatabase(DB_NAME);

        //db should have been removed from cache
        assertNull(cacheDecorator.databaseCache.getIfPresent(DB_NAME));
        assertNull(cacheDecorator.databasesCache.getIfPresent(DB_NAME));
        verify(glueMetastore, times(1)).deleteDatabase(DB_NAME);
    }

    @Test
    public void testCreateDatabaseWhenCacheDisabled() {
        //disable cache
        when(hiveConf.getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false)).thenReturn(false);
        AWSGlueMetastoreCacheDecorator cacheDecorator =
                new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
        DatabaseInput dbInput = new DatabaseInput();
        doNothing().when(glueMetastore).createDatabase(dbInput);
        cacheDecorator.createDatabase(dbInput);
        assertNull(cacheDecorator.databasesCache);
        verify(glueMetastore, times(1)).createDatabase(dbInput);
    }

    @Test
    public void testCreateDatabaseWhenCacheEnabled() {
        DatabaseInput dbInput = new DatabaseInput();
        List<String> names = new ArrayList<>();
        AWSGlueMetastoreCacheDecorator cacheDecorator =
                new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
        cacheDecorator.databasesCache.put(DATABASES_CACHE_KEY, names);
        doNothing().when(glueMetastore).createDatabase(dbInput);

        cacheDecorator.createDatabase(dbInput);

        verify(glueMetastore, times(1)).createDatabase(dbInput);
        assertEquals(cacheDecorator.databasesCache.size(), 0L);
    }

    @Test
    public void testGetAllDatabasesWhenCacheDisabled() {
        //disable cache
        when(hiveConf.getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false)).thenReturn(false);
        Database db1 = new Database();
        Database db2 = new Database();
        List<Database> dbs = Arrays.asList(db1, db2);
        AWSGlueMetastoreCacheDecorator cacheDecorator =
                new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
        when(glueMetastore.getAllDatabases()).thenReturn(dbs);
        assertEquals(dbs, cacheDecorator.getAllDatabases());
        assertNull(cacheDecorator.databasesCache);
        verify(glueMetastore, times(1)).getAllDatabases();
    }

    @Test
    public void testGetAllDatabasesWhenCacheEnabledAndCacheMiss() {
        Database db1 = new Database();
        List<Database> dbs = Arrays.asList(db1);
        List<String> dbNames = Arrays.asList(db1.getName());
        AWSGlueMetastoreCacheDecorator cacheDecorator =
                new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
        assertNotNull(cacheDecorator.databasesCache);
        assertNotNull(cacheDecorator.databaseCache);
        Cache dbsCache = mock(Cache.class);
        Cache dbCache = mock(Cache.class);
        cacheDecorator.databasesCache = dbsCache;
        cacheDecorator.databaseCache = dbCache;

        when(dbsCache.size()).thenReturn(0L);
        when(glueMetastore.getAllDatabases()).thenReturn(dbs);

        assertEquals(dbs, cacheDecorator.getAllDatabases());

        verify(glueMetastore, times(1)).getAllDatabases();
        verify(dbsCache, times(1)).size();
        verify(dbsCache, times(1)).put(DATABASES_CACHE_KEY, dbNames);
    }

    @Test
    public void testGetAllDatabasesWhenCacheEnabledAndCacheMiss2() {
        Database db1 = new Database();
        Database db2 = new Database();
        List<Database> dbs = Arrays.asList(db1, db2);
        List<String> dbNames = Arrays.asList(db1.getName(), db2.getName());
        AWSGlueMetastoreCacheDecorator cacheDecorator =
                new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
        assertNotNull(cacheDecorator.databasesCache);
        assertNotNull(cacheDecorator.databaseCache);
        Cache dbsCache = mock(Cache.class);
        Cache dbCache = mock(Cache.class);
        cacheDecorator.databasesCache = dbsCache;
        cacheDecorator.databaseCache = dbCache;

        when(dbsCache.size()).thenReturn(1L);
        when(dbsCache.getIfPresent(DATABASES_CACHE_KEY)).thenReturn(dbNames);
        when(dbCache.getIfPresent(db1.getName())).thenReturn(db1);
        when(dbCache.getIfPresent(db2.getName())).thenReturn(null);
        when(glueMetastore.getAllDatabases()).thenReturn(dbs);

        assertEquals(dbs, cacheDecorator.getAllDatabases());

        verify(glueMetastore, times(1)).getAllDatabases();
        verify(dbsCache, times(1)).size();
        verify(dbsCache, times(1)).put(DATABASES_CACHE_KEY, dbNames);
    }

    @Test
    public void testGetAllDatabasesWhenCacheEnabledAndCacheHit() {
        Database db1 = new Database();
        Database db2 = new Database();
        List<Database> dbs = Arrays.asList(db1, db2);
        List<String> dbNames = Arrays.asList(db1.getName(), db2.getName());


        AWSGlueMetastoreCacheDecorator cacheDecorator =
                new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
        assertNotNull(cacheDecorator.databasesCache);
        Cache dbsCache = mock(Cache.class);
        Cache dbCache = mock(Cache.class);
        cacheDecorator.databasesCache = dbsCache;
        cacheDecorator.databaseCache = dbCache;

        when(dbsCache.size()).thenReturn(2L);
        when(dbsCache.getIfPresent(DATABASES_CACHE_KEY)).thenReturn(dbNames);
        when(dbCache.getIfPresent(db1.getName())).thenReturn(db1);
        when(dbCache.getIfPresent(db2.getName())).thenReturn(db2);

        assertEquals(dbs, cacheDecorator.getAllDatabases());

        verify(dbsCache, times(1)).size();
    }

    @Test
    public void testGetTableWhenCacheDisabled() {
        //disable cache
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
    public void testUpdateTableWhenCacheDisabled() {
        //disable cache
        when(hiveConf.getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false)).thenReturn(false);
        TableInput tableInput = new TableInput();
        AWSGlueMetastoreCacheDecorator cacheDecorator =
                new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
        doNothing().when(glueMetastore).updateTable(TABLE_NAME, tableInput);
        cacheDecorator.updateTable(TABLE_NAME, tableInput);
        assertNull(cacheDecorator.tableCache);
        verify(glueMetastore, times(1)).updateTable(TABLE_NAME, tableInput);
    }

    @Test
    public void testUpdateTableWhenCacheEnabled() {
        TableInput tableInput = new TableInput();
        tableInput.setName(TABLE_NAME);
        AWSGlueMetastoreCacheDecorator cacheDecorator =
                new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);

        cacheDecorator.tableCache.put(TABLE_IDENTIFIER, new Table());
        doNothing().when(glueMetastore).updateTable(DB_NAME, tableInput);

        cacheDecorator.updateTable(DB_NAME, tableInput);

        //table should have been removed from cache
        assertNull(cacheDecorator.tableCache.getIfPresent(TABLE_IDENTIFIER));
        verify(glueMetastore, times(1)).updateTable(DB_NAME, tableInput);
    }

    @Test
    public void testDeleteTableWhenCacheDisabled() {
        //disable cache
        when(hiveConf.getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false)).thenReturn(false);
        AWSGlueMetastoreCacheDecorator cacheDecorator =
                new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
        doNothing().when(glueMetastore).deleteTable(DB_NAME, TABLE_NAME);
        cacheDecorator.deleteTable(DB_NAME, TABLE_NAME);
        assertNull(cacheDecorator.tableCache);
        verify(glueMetastore, times(1)).deleteTable(DB_NAME, TABLE_NAME);
    }

    @Test
    public void testDeleteTableWhenCacheEnabled() {
        DatabaseInput dbInput = new DatabaseInput();
        AWSGlueMetastoreCacheDecorator cacheDecorator =
                new AWSGlueMetastoreCacheDecorator(hiveConf, glueMetastore);
        cacheDecorator.tableCache.put(TABLE_IDENTIFIER, new Table());
        doNothing().when(glueMetastore).deleteDatabase(DB_NAME);

        cacheDecorator.deleteTable(DB_NAME, TABLE_NAME);

        //table should have been removed from cache
        assertNull(cacheDecorator.tableCache.getIfPresent(TABLE_IDENTIFIER));
        verify(glueMetastore, times(1)).deleteTable(DB_NAME, TABLE_NAME);
    }

}