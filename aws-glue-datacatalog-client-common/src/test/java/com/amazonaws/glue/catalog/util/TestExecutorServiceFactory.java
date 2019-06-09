package com.amazonaws.glue.catalog.util;

import com.amazonaws.glue.catalog.metastore.ExecutorServiceFactory;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.concurrent.ExecutorService;

public class TestExecutorServiceFactory implements ExecutorServiceFactory {
    private static ExecutorService execService = new TestExecutorService(1, new ThreadFactoryBuilder().build());

    @Override
    public ExecutorService getExecutorService(HiveConf conf) {
        return execService;
    }
}
