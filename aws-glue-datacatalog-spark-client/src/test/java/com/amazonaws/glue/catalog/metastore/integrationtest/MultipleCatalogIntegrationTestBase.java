package com.amazonaws.glue.catalog.metastore.integrationtest;

import com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient;
import com.amazonaws.glue.catalog.metastore.AWSGlueClientFactory;
import com.amazonaws.glue.catalog.metastore.GlueClientFactory;
import com.amazonaws.glue.catalog.util.AWSGlueConfig;
import com.amazonaws.services.glue.AWSGlue;
import com.google.common.base.Strings;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Base class for integration test to check that multiple Glue catalogs can be accessed using single MetastoreClient.
 * To be able to run such test you have to make the following set up:
 * 1. Retrieve credentials of AWS account (lets say account A) which will be used to authenticate your test.
 * 2. Set credentials and region of account A the following variables: AWS_SECRET_KEY, AWS_ACCESS_KEY, AWS_REGION.
 * 3. Set up permissions in another AWS account (lets say account B) that account A has access to account B.
 * 3.1 Log in to AWS Console under account B, choose AWS Glue service and go to Settings.
 * 3.2 Set the following policy:
 {
 "Version" : "2012-10-17",
 "Statement" : [ {
 "Effect" : "Allow",
 "Principal" : {
 "AWS" : "arn:aws:iam::<account A>:root"
 },
 "Action" : "glue:*",
 "Resource" : "arn:aws:glue:us-east-1:<account B>:*"
 } ]
 }
 * 4. Set account B id as a value of ANOTHER_GLUE_CATALOG_ID variable.
 *
 * If another account preparation is not an option for you then you can skip #3 and set account A as a value of
 * ANOTHER_GLUE_CATALOG_ID variable. In this case test will pass but you won't test a real multiple catalog access.
 */
public class MultipleCatalogIntegrationTestBase {
    private static final String ANOTHER_GLUE_CATALOG_ID = "ANOTHER_GLUE_CATALOG_ID";
    private static final String CATALOG_SEPARATOR = "/-/";

    protected IMetaStoreClient metastoreClient;
    protected AWSGlue glueClient;
    protected String anotherCatalogId;

    protected void setup() throws MetaException, TException {
        HiveConf conf = new HiveConf();
        conf.set(AWSGlueConfig.AWS_GLUE_CATALOG_SEPARATOR, CATALOG_SEPARATOR);
        Warehouse wh = mock(Warehouse.class);
        Path tmpPath = new Path("/db");
        when(wh.getDefaultDatabasePath(anyString())).thenReturn(tmpPath);
        when(wh.getDnsPath(any(Path.class))).thenReturn(tmpPath);
        when(wh.isDir(any(Path.class))).thenReturn(true);

        GlueClientFactory clientFactory = new AWSGlueClientFactory(conf);
        glueClient = clientFactory.newClient();

        metastoreClient = new AWSCatalogMetastoreClient.Builder().withHiveConf(conf).withWarehouse(wh)
                .withClientFactory(clientFactory).build();

        anotherCatalogId = System.getenv(ANOTHER_GLUE_CATALOG_ID);
        if (Strings.isNullOrEmpty(anotherCatalogId)) {
            throw new RuntimeException(String.format(
                    "Environment variable %s is not set. " +
                            "Please read comment for %s class to understand what value should be set there.",
                    ANOTHER_GLUE_CATALOG_ID, this.getClass().getName()));
        }
    }

    protected String constructDbName(String originalDbName) {
        return String.format("%s%s%s", anotherCatalogId, CATALOG_SEPARATOR, originalDbName);
    }

}
