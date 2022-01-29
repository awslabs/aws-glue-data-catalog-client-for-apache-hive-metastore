package com.amazonaws.glue.catalog.metastore;

import org.apache.hadoop.conf.Configuration;

import com.amazonaws.auth.AWSCredentialsProvider;

public interface AWSCredentialsProviderFactory {

  AWSCredentialsProvider buildAWSCredentialsProvider(Configuration conf);
}
