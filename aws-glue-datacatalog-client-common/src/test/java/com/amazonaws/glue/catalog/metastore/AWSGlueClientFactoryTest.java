package com.amazonaws.glue.catalog.metastore;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.glue.AWSGlue;

import org.apache.hadoop.conf.Configuration;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_CATALOG_CREDENTIALS_PROVIDER_FACTORY_CLASS;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_CATALOG_SEPARATOR;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_CONNECTION_TIMEOUT;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_ENDPOINT;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_MAX_CONNECTIONS;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_MAX_RETRY;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_SOCKET_TIMEOUT;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_REGION;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.DEFAULT_CONNECTION_TIMEOUT;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.DEFAULT_MAX_CONNECTIONS;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.DEFAULT_MAX_RETRY;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.DEFAULT_SOCKET_TIMEOUT;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AWSGlueClientFactoryTest {

  private static final String FAKE_ACCESS_KEY = "accessKey";
  private static final String FAKE_SECRET_KEY = "secretKey";
  private static final String FAKE_SESSION_TOKEN = "sessionToken";

  private AWSGlueClientFactory glueClientFactory;
  private Configuration conf;

  @Before
  public void setup() {
    conf = spy(new Configuration());
    glueClientFactory = new AWSGlueClientFactory(conf);
  }

  @Test
  public void testGlueClientConstructionWithHiveConfig() throws Exception {
    System.setProperty(AWS_REGION, "");
    System.setProperty(AWS_GLUE_ENDPOINT, "");
    System.setProperty(AWS_GLUE_CATALOG_SEPARATOR, "");
    when(conf.get(AWS_GLUE_ENDPOINT)).thenReturn("endpoint");
    when(conf.get(AWS_REGION)).thenReturn("us-west-1");
    when(conf.get(AWS_GLUE_CATALOG_SEPARATOR)).thenReturn("/");

    AWSGlue glueClient = glueClientFactory.newClient();

    assertNotNull(glueClient);

    // client reads hive conf for region & endpoint
    verify(conf, atLeastOnce()).get(AWS_GLUE_ENDPOINT);
    verify(conf, atLeastOnce()).get(AWS_REGION);
    verify(conf, atLeastOnce()).get(AWS_GLUE_CATALOG_SEPARATOR);
  }

  @Test
  public void testGlueClientContructionWithAWSConfig() throws Exception {
    glueClientFactory.newClient();
    verify(conf, atLeastOnce()).getInt(AWS_GLUE_MAX_RETRY, DEFAULT_MAX_RETRY);
    verify(conf, atLeastOnce()).getInt(AWS_GLUE_MAX_CONNECTIONS, DEFAULT_MAX_CONNECTIONS);
    verify(conf, atLeastOnce()).getInt(AWS_GLUE_SOCKET_TIMEOUT, DEFAULT_SOCKET_TIMEOUT);
    verify(conf, atLeastOnce()).getInt(AWS_GLUE_CONNECTION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT);
  }

  @Test
  public void testGlueClientConstructionWithSystemProperty() throws Exception {
    System.setProperty(AWS_REGION, "us-east-1");
    System.setProperty(AWS_GLUE_ENDPOINT, "endpoint");
    System.setProperty(AWS_GLUE_CATALOG_SEPARATOR, "/");

    AWSGlue glueClient = glueClientFactory.newClient();

    assertNotNull(glueClient);

    // client has no interactions with the hive conf since system property is set
    verify(conf, never()).get(AWS_GLUE_ENDPOINT);
    verify(conf, never()).get(AWS_REGION);
    verify(conf, never()).get(AWS_GLUE_CATALOG_SEPARATOR);
  }

  @Test
  public void testClientConstructionWithSessionCredentialsProviderFactory() throws Exception {
    System.setProperty("aws.region", "us-west-2");
    conf.setStrings(SessionCredentialsProviderFactory.AWS_ACCESS_KEY_CONF_VAR, FAKE_ACCESS_KEY);
    conf.setStrings(SessionCredentialsProviderFactory.AWS_SECRET_KEY_CONF_VAR, FAKE_SECRET_KEY);
    conf.setStrings(SessionCredentialsProviderFactory.AWS_SESSION_TOKEN_CONF_VAR, FAKE_SESSION_TOKEN);

    conf.setStrings(AWS_CATALOG_CREDENTIALS_PROVIDER_FACTORY_CLASS,
        SessionCredentialsProviderFactory.class.getCanonicalName());

    AWSGlue glueClient = glueClientFactory.newClient();

    assertNotNull(glueClient);

    verify(conf, atLeastOnce()).get(SessionCredentialsProviderFactory.AWS_ACCESS_KEY_CONF_VAR);
    verify(conf, atLeastOnce()).get(SessionCredentialsProviderFactory.AWS_SECRET_KEY_CONF_VAR);
    verify(conf, atLeastOnce()).get(SessionCredentialsProviderFactory.AWS_SESSION_TOKEN_CONF_VAR);
  }

  @Test
  public void testCredentialsCreatedBySessionCredentialsProviderFactory() throws Exception {
    conf.setStrings(SessionCredentialsProviderFactory.AWS_ACCESS_KEY_CONF_VAR, FAKE_ACCESS_KEY);
    conf.setStrings(SessionCredentialsProviderFactory.AWS_SECRET_KEY_CONF_VAR, FAKE_SECRET_KEY);
    conf.setStrings(SessionCredentialsProviderFactory.AWS_SESSION_TOKEN_CONF_VAR, FAKE_SESSION_TOKEN);

    SessionCredentialsProviderFactory factory = new SessionCredentialsProviderFactory();
    AWSCredentialsProvider provider = factory.buildAWSCredentialsProvider(conf);
    AWSCredentials credentials = provider.getCredentials();

    assertThat(credentials, instanceOf(BasicSessionCredentials.class));

    BasicSessionCredentials sessionCredentials = (BasicSessionCredentials) credentials;

    assertEquals(FAKE_ACCESS_KEY, sessionCredentials.getAWSAccessKeyId());
    assertEquals(FAKE_SECRET_KEY, sessionCredentials.getAWSSecretKey());
    assertEquals(FAKE_SESSION_TOKEN, sessionCredentials.getSessionToken());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingAccessKeyWithSessionCredentialsProviderFactory() throws Exception {
    SessionCredentialsProviderFactory factory = new SessionCredentialsProviderFactory();
    factory.buildAWSCredentialsProvider(conf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingSecretKey() throws Exception {
    SessionCredentialsProviderFactory factory = new SessionCredentialsProviderFactory();
    conf.setStrings(SessionCredentialsProviderFactory.AWS_ACCESS_KEY_CONF_VAR, FAKE_ACCESS_KEY);
    factory.buildAWSCredentialsProvider(conf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingSessionTokenKey() throws Exception {
    SessionCredentialsProviderFactory factory = new SessionCredentialsProviderFactory();
    conf.setStrings(SessionCredentialsProviderFactory.AWS_ACCESS_KEY_CONF_VAR, FAKE_ACCESS_KEY);
    conf.setStrings(SessionCredentialsProviderFactory.AWS_SECRET_KEY_CONF_VAR, FAKE_SECRET_KEY);
    factory.buildAWSCredentialsProvider(conf);
  }

}
