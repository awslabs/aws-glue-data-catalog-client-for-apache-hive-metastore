package com.amazonaws.glue.catalog.metastore;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;

import org.apache.hadoop.conf.Configuration;

import static com.google.common.base.Preconditions.checkArgument;

public class SessionCredentialsProviderFactory implements AWSCredentialsProviderFactory {

  public final static String AWS_ACCESS_KEY_CONF_VAR = "hive.aws_session_access_id";
  public final static String AWS_SECRET_KEY_CONF_VAR = "hive.aws_session_secret_key";
  public final static String AWS_SESSION_TOKEN_CONF_VAR = "hive.aws_session_token";
  
  @Override
  public AWSCredentialsProvider buildAWSCredentialsProvider(Configuration conf) {

    checkArgument(conf != null, "conf cannot be null.");
    
    String accessKey = conf.get(AWS_ACCESS_KEY_CONF_VAR);
    String secretKey = conf.get(AWS_SECRET_KEY_CONF_VAR);
    String sessionToken = conf.get(AWS_SESSION_TOKEN_CONF_VAR);
    
    checkArgument(accessKey != null, AWS_ACCESS_KEY_CONF_VAR + " must be set.");
    checkArgument(secretKey != null, AWS_SECRET_KEY_CONF_VAR + " must be set.");
    checkArgument(sessionToken != null, AWS_SESSION_TOKEN_CONF_VAR + " must be set.");
    
    AWSSessionCredentials credentials = new BasicSessionCredentials(accessKey, secretKey, sessionToken);
    
    return new StaticCredentialsProvider(credentials);
  }
}
