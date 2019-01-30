package com.amazonaws.glue.catalog.metastore;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;

import org.apache.hadoop.hive.conf.HiveConf;

import static com.google.common.base.Preconditions.checkArgument;

public class SessionCredentialsProviderFactory implements AWSCredentialsProviderFactory {

  public final static String AWS_ACCESS_KEY_CONF_VAR = "hive.aws_session_access_id";
  public final static String AWS_SECRET_KEY_CONF_VAR = "hive.aws_session_secret_key";
  public final static String AWS_SESSION_TOKEN_CONF_VAR = "hive.aws_session_token";
  
  @Override
  public AWSCredentialsProvider buildAWSCredentialsProvider(HiveConf hiveConf) {

    checkArgument(hiveConf != null, "hiveConf cannot be null.");
    
    String accessKey = hiveConf.get(AWS_ACCESS_KEY_CONF_VAR);
    String secretKey = hiveConf.get(AWS_SECRET_KEY_CONF_VAR);
    String sessionToken = hiveConf.get(AWS_SESSION_TOKEN_CONF_VAR);
    
    checkArgument(accessKey != null, AWS_ACCESS_KEY_CONF_VAR + " must be set.");
    checkArgument(secretKey != null, AWS_SECRET_KEY_CONF_VAR + " must be set.");
    checkArgument(sessionToken != null, AWS_SESSION_TOKEN_CONF_VAR + " must be set.");
    
    AWSSessionCredentials credentials = new BasicSessionCredentials(accessKey, secretKey, sessionToken);
    
    return new StaticCredentialsProvider(credentials);
  }
}
