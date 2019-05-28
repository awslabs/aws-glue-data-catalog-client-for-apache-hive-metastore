package com.amazonaws.glue.catalog.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hive.conf.HiveConf;
import

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class ThreadExecutorFactory {
    public static final int NUM_EXECUTOR_THREADS = 5;
    public static final String GLUE_METASTORE_DELEGATE_THREADPOOL_NAME_FORMAT = "glue-metastore-delegate-%d";
    public static final String CUSTOM_EXECUTOR_CONF = "hive.metastore.custom.executor.class";

    private static ExecutorService delegateThreadPool = null;

    public static ExecutorService getGlueMetastoreDelegateThreadPool(HiveConf conf) {
      if (delegateThreadPool == null ) {
        synchronized (delegateThreadPool) {

          if (delegateThreadPool == null ) {
              ThreadFactory threadFactory = new ThreadFactoryBuilder()
                      .setNameFormat(GLUE_METASTORE_DELEGATE_THREADPOOL_NAME_FORMAT)
                      .setDaemon(true).build();
              String customExecutorClass = conf.get(CUSTOM_EXECUTOR_CONF);
              if (customExecutorClass != null && !customExecutorClass.isEmpty()){
                  delegateThreadPool = (ExecutorService) getInstanceByReflection(
                          customExecutorClass, NUM_EXECUTOR_THREADS, threadFactory);
              } else {
                  delegateThreadPool = Executors.newFixedThreadPool(NUM_EXECUTOR_THREADS, threadFactory);
              }
          }
        }
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
        } catch( ClassNotFoundException ce ){

        } catch ( NoSuchMethodException ne ) {

        } catch( InstantiationException ie ) {

        } catch (IllegalAccessException iae ){

        } catch( InvocationTargetException ite ){

        }
    }
}
