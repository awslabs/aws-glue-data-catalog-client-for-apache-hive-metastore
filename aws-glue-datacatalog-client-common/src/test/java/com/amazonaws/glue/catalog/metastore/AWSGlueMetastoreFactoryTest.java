package com.amazonaws.glue.catalog.metastore;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Before;
import org.junit.Test;

import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_DB_CACHE_ENABLE;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_TABLE_CACHE_ENABLE;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_ENDPOINT;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_TABLE_CACHE_SIZE;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_TABLE_CACHE_TTL_MINS;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_DB_CACHE_SIZE;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_DB_CACHE_TTL_MINS;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_REGION;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertTrue;

public class AWSGlueMetastoreFactoryTest {

    private AWSGlueMetastoreFactory awsGlueMetastoreFactory;
    private HiveConf hiveConf;

    @Before
    public void setUp() {
        awsGlueMetastoreFactory = new AWSGlueMetastoreFactory();
        hiveConf = spy(new HiveConf());

        // these configs are needed for AWSGlueClient to get initialized
        System.setProperty(AWS_REGION, "");
        System.setProperty(AWS_GLUE_ENDPOINT, "");
        when(hiveConf.get(AWS_GLUE_ENDPOINT)).thenReturn("endpoint");
        when(hiveConf.get(AWS_REGION)).thenReturn("us-west-1");

        // these configs are needed for AWSGlueMetastoreCacheDecorator to get initialized
        when(hiveConf.getInt(AWS_GLUE_DB_CACHE_SIZE, 0)).thenReturn(1);
        when(hiveConf.getInt(AWS_GLUE_DB_CACHE_TTL_MINS, 0)).thenReturn(1);
        when(hiveConf.getInt(AWS_GLUE_TABLE_CACHE_SIZE, 0)).thenReturn(1);
        when(hiveConf.getInt(AWS_GLUE_TABLE_CACHE_TTL_MINS, 0)).thenReturn(1);
    }

    @Test
    public void testNewMetastoreWhenCacheDisabled() throws Exception {
        when(hiveConf.getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false)).thenReturn(false);
        when(hiveConf.getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false)).thenReturn(false);
        assertTrue(DefaultAWSGlueMetastore.class.equals(
                awsGlueMetastoreFactory.newMetastore(hiveConf).getClass()));
        verify(hiveConf, atLeastOnce()).getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false);
        verify(hiveConf, atLeastOnce()).getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false);
    }

    @Test
    public void testNewMetastoreWhenTableCacheEnabled() throws Exception {
        when(hiveConf.getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false)).thenReturn(false);
        when(hiveConf.getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false)).thenReturn(true);
        assertTrue(AWSGlueMetastoreCacheDecorator.class.equals(
                awsGlueMetastoreFactory.newMetastore(hiveConf).getClass()));
        verify(hiveConf, atLeastOnce()).getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false);
        verify(hiveConf, atLeastOnce()).getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false);
    }

    @Test
    public void testNewMetastoreWhenDBCacheEnabled() throws Exception {
        when(hiveConf.getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false)).thenReturn(true);
        when(hiveConf.getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false)).thenReturn(true);
        assertTrue(AWSGlueMetastoreCacheDecorator.class.equals(
                awsGlueMetastoreFactory.newMetastore(hiveConf).getClass()));
        verify(hiveConf, atLeastOnce()).getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false);
        verify(hiveConf, atLeastOnce()).getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false);
    }

    @Test
    public void testNewMetastoreWhenAllCacheEnabled() throws Exception {
        when(hiveConf.getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false)).thenReturn(true);
        when(hiveConf.getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false)).thenReturn(true);
        assertTrue(AWSGlueMetastoreCacheDecorator.class.equals(
                awsGlueMetastoreFactory.newMetastore(hiveConf).getClass()));
        verify(hiveConf, atLeastOnce()).getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false);
        verify(hiveConf, atLeastOnce()).getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false);
    }

}