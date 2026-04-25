package com.amazonaws.glue.catalog.metastore;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import org.apache.hadoop.conf.Configuration;

import static com.google.common.base.Preconditions.checkArgument;

public class SessionCredentialsProviderFactory implements AWSCredentialsProviderFactory {

  public final static String AWS_ACCESS_KEY_CONF_VAR = "hive.aws_session_access_id";
  public final static String AWS_SECRET_KEY_CONF_VAR = "hive.aws_session_secret_key";
  public final static String AWS_SESSION_TOKEN_CONF_VAR = "hive.aws_session_token";
  
  @Override
  public AwsCredentialsProvider buildAWSCredentialsProvider(Configuration conf) {

    checkArgument(conf != null, "conf cannot be null.");
    
    String accessKey = conf.get(AWS_ACCESS_KEY_CONF_VAR);
    String secretKey = conf.get(AWS_SECRET_KEY_CONF_VAR);
    String sessionToken = conf.get(AWS_SESSION_TOKEN_CONF_VAR);
    
    checkArgument(accessKey != null, AWS_ACCESS_KEY_CONF_VAR + " must be set.");
    checkArgument(secretKey != null, AWS_SECRET_KEY_CONF_VAR + " must be set.");
    checkArgument(sessionToken != null, AWS_SESSION_TOKEN_CONF_VAR + " must be set.");
    
    AwsSessionCredentials credentials = AwsSessionCredentials.create(accessKey, secretKey, sessionToken);
    
    return StaticCredentialsProvider.create(credentials);
  }
}
