package com.amazonaws.glue.catalog.metastore;

import org.apache.hadoop.hive.conf.HiveConf;

import java.util.concurrent.ExecutorService;

/*
 * Interface for creating an ExecutorService
 */
public interface ExecutorServiceFactory {
    public ExecutorService getExecutorService(HiveConf conf);
}
