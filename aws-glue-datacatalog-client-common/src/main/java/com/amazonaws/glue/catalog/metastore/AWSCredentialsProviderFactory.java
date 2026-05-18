package com.amazonaws.glue.catalog.metastore;

import org.apache.hadoop.conf.Configuration;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public interface AWSCredentialsProviderFactory {

  AwsCredentialsProvider buildAWSCredentialsProvider(Configuration conf);
}
