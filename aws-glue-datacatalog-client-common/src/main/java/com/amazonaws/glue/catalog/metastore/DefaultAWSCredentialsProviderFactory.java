package com.amazonaws.glue.catalog.metastore;

import org.apache.hadoop.conf.Configuration;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

public class DefaultAWSCredentialsProviderFactory implements
    AWSCredentialsProviderFactory {

  @Override
  public AwsCredentialsProvider buildAWSCredentialsProvider(Configuration conf) {
    return DefaultCredentialsProvider.create();
  }

}
