package com.amazonaws.glue.catalog.util;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

public class TestExecutorService extends ScheduledThreadPoolExecutor {

    public TestExecutorService(int corePoolSize, ThreadFactory factory) {
        super(corePoolSize, factory);
    }
}