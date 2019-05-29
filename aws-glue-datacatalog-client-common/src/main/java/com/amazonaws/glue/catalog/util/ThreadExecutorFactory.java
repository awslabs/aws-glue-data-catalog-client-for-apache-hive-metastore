package com.amazonaws.glue.catalog.util;

import com.amazonaws.glue.catalog.metastore.AWSGlueClientFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.log4j.Logger;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutorService;

import java.util.concurrent.ThreadFactory;

public class ThreadExecutorFactory {
    private static final Logger logger = Logger.getLogger(ThreadExecutorFactory.class);
    public static final String CUSTOM_EXECUTOR_CONF = "hive.metastore.custom.executor.class";

    public static ExecutorService getCustomThreadPool(HiveConf conf, int numThreads, ThreadFactory threadFactory) {
      ExecutorService delegateThreadPool = null;
      String customExecutorClass = conf.get(CUSTOM_EXECUTOR_CONF);
        if (customExecutorClass != null && !customExecutorClass.isEmpty()){
          delegateThreadPool = (ExecutorService) getInstanceByReflection(customExecutorClass, numThreads, threadFactory);
        }
      return delegateThreadPool;
    }

    public static Object getInstanceByReflection(String className, int arg1, ThreadFactory arg2) {
        try {
            Class[] argTypes = {Integer.class, java.util.concurrent.ThreadFactory.class};
            Class classDefinition = Class.forName(className);
            Constructor cons = classDefinition.getConstructor(argTypes);
            Object[] args = {arg1, arg2};
            return cons.newInstance(args);
        } catch( ClassNotFoundException e ){
            logger.warn("Exception in initializing custom executor ",e);
        } catch ( NoSuchMethodException e ) {
            logger.warn("Exception in initializing custom executor ", e);
        } catch( InstantiationException e ) {
            logger.warn("Exception in initializing custom executor ", e);
        } catch (IllegalAccessException e ){
            logger.warn("Exception in initializing custom executor ", e);
        } catch( InvocationTargetException e ){
            logger.warn("Exception in initializing custom executor ", e);
        }
        return null;
    }
}
