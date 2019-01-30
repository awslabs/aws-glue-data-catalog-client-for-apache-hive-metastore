package com.amazonaws.glue.catalog.util;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.glue.catalog.metastore.GlueClientFactory;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;

public final class GlueTestClientFactory implements GlueClientFactory {

  private static final int SC_GATEWAY_TIMEOUT = 504;

  @Override
  public AWSGlue newClient() throws MetaException {
    AWSGlueClientBuilder glueClientBuilder = AWSGlueClientBuilder.standard()
        .withClientConfiguration(createGatewayTimeoutRetryableConfiguration())
        .withCredentials(new DefaultAWSCredentialsProviderChain());

    String endpoint = System.getProperty("endpoint");
    if (StringUtils.isNotBlank(endpoint)) {
      glueClientBuilder.setEndpointConfiguration(new EndpointConfiguration(endpoint, null));
    }

    return glueClientBuilder.build();
  }

  private static ClientConfiguration createGatewayTimeoutRetryableConfiguration() {
    ClientConfiguration retryableConfig = new ClientConfiguration();
    RetryPolicy.RetryCondition retryCondition = new PredefinedRetryPolicies.SDKDefaultRetryCondition() {
      @Override
      public boolean shouldRetry(AmazonWebServiceRequest originalRequest, AmazonClientException exception,
                                 int retriesAttempted) {
        if (super.shouldRetry(originalRequest, exception, retriesAttempted)) {
          return true;
        }
        if (exception != null && exception instanceof AmazonServiceException) {
          AmazonServiceException ase = (AmazonServiceException) exception;
          if (ase.getStatusCode() == SC_GATEWAY_TIMEOUT) {
            return true;
          }
        }
        return false;
      }
    };
    RetryPolicy retryPolicy = new RetryPolicy(retryCondition, PredefinedRetryPolicies.DEFAULT_BACKOFF_STRATEGY,
                                                     PredefinedRetryPolicies.DEFAULT_MAX_ERROR_RETRY, true);
    retryableConfig.setRetryPolicy(retryPolicy);
    return retryableConfig;
  }

}
