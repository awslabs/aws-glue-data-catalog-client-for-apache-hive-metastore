package com.amazonaws.glue.catalog.metastore;

import org.apache.hadoop.hive.conf.HiveConf;

import com.amazonaws.auth.AWSCredentialsProvider;

public interface AWSCredentialsProviderFactory {

  AWSCredentialsProvider buildAWSCredentialsProvider(HiveConf hiveConf);
}
