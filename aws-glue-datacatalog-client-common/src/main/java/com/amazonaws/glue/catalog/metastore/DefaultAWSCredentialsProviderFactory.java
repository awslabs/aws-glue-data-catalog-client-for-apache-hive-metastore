package com.amazonaws.glue.catalog.metastore;

import org.apache.hadoop.hive.conf.HiveConf;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;

public class DefaultAWSCredentialsProviderFactory implements
    AWSCredentialsProviderFactory {

  @Override
  public AWSCredentialsProvider buildAWSCredentialsProvider(HiveConf hiveConf) {
    return new DefaultAWSCredentialsProviderChain();
  }

}
