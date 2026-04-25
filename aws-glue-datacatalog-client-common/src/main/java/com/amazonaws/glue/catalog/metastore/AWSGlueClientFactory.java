package com.amazonaws.glue.catalog.metastore;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.GlueClientBuilder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

import java.net.URI;
import java.time.Duration;

import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_CATALOG_CREDENTIALS_PROVIDER_FACTORY_CLASS;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_CATALOG_SEPARATOR;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_CONNECTION_TIMEOUT;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_ENDPOINT;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_MAX_RETRY;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_SOCKET_TIMEOUT;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_REGION;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.DEFAULT_CONNECTION_TIMEOUT;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.DEFAULT_MAX_RETRY;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.DEFAULT_SOCKET_TIMEOUT;

public final class AWSGlueClientFactory implements GlueClientFactory {

  private static final Logger logger = Logger.getLogger(AWSGlueClientFactory.class);

  private final Configuration conf;

  public AWSGlueClientFactory(Configuration conf) {
    Preconditions.checkNotNull(conf, "Configuration cannot be null");
    this.conf = conf;
  }

  @Override
  public GlueClient newClient() throws MetaException {
    try {
      GlueClientBuilder glueClientBuilder = GlueClient.builder()
          .credentialsProvider(getAWSCredentialsProvider(conf));

      String regionStr = getProperty(AWS_REGION, conf);
      String glueEndpoint = getProperty(AWS_GLUE_ENDPOINT, conf);

      // ClientBuilder only allows one of EndpointConfiguration or Region to be set
      if (StringUtils.isNotBlank(glueEndpoint)) {
        logger.info("Setting glue service endpoint to " + glueEndpoint);
        glueClientBuilder.endpointOverride(URI.create(glueEndpoint));
        if (StringUtils.isNotBlank(regionStr)) {
          glueClientBuilder.region(Region.of(regionStr));
        }
      } else if (StringUtils.isNotBlank(regionStr)) {
        logger.info("Setting region to : " + regionStr);
        glueClientBuilder.region(Region.of(regionStr));
      } else {
        try {
          Region currentRegion = new DefaultAwsRegionProviderChain().getRegion();
          logger.info("Using region from default provider chain : " + currentRegion);
          glueClientBuilder.region(currentRegion);
        } catch (Exception e) {
          logger.info("No region info found, using SDK default region: us-east-1");
          glueClientBuilder.region(Region.US_EAST_1);
        }
      }

      glueClientBuilder.overrideConfiguration(buildClientOverrideConfiguration(conf));
      return glueClientBuilder.build();
    } catch (Exception e) {
      String message = "Unable to build GlueClient: " + e;
      logger.error(message);
      throw new MetaException(message);
    }
  }

  @VisibleForTesting
  AwsCredentialsProvider getAWSCredentialsProvider(Configuration conf) {

    Class<? extends AWSCredentialsProviderFactory> providerFactoryClass = conf
        .getClass(AWS_CATALOG_CREDENTIALS_PROVIDER_FACTORY_CLASS,
            DefaultAWSCredentialsProviderFactory.class).asSubclass(
            AWSCredentialsProviderFactory.class);
    AWSCredentialsProviderFactory provider = ReflectionUtils.newInstance(
        providerFactoryClass, conf);
    return provider.buildAWSCredentialsProvider(conf);
  }

  private ClientOverrideConfiguration buildClientOverrideConfiguration(Configuration conf) {
    return ClientOverrideConfiguration.builder()
        .retryPolicy(RetryPolicy.builder()
            .numRetries(conf.getInt(AWS_GLUE_MAX_RETRY, DEFAULT_MAX_RETRY))
            .build())
        .apiCallTimeout(Duration.ofMillis(
            conf.getInt(AWS_GLUE_CONNECTION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT)
            + conf.getInt(AWS_GLUE_SOCKET_TIMEOUT, DEFAULT_SOCKET_TIMEOUT)))
        .apiCallAttemptTimeout(Duration.ofMillis(
            conf.getInt(AWS_GLUE_SOCKET_TIMEOUT, DEFAULT_SOCKET_TIMEOUT)))
        .build();
  }

  private static String getProperty(String propertyName, Configuration conf) {
    return Strings.isNullOrEmpty(System.getProperty(propertyName)) ?
        conf.get(propertyName) : System.getProperty(propertyName);
  }
}
