package com.amazonaws.glue.catalog.metastore;

import org.apache.hadoop.conf.Configuration;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;

public class DefaultAWSCredentialsProviderFactory implements
    AWSCredentialsProviderFactory {

  @Override
  public AWSCredentialsProvider buildAWSCredentialsProvider(Configuration conf) {
    return new DefaultAWSCredentialsProviderChain();
  }

}
