package com.amazonaws.glue.catalog.metastore;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;

public class AWSGlueDataCatalogHiveClientFactory implements HiveMetastoreClientFactory {

    @Override
    public IMetaStoreClient getHiveMetastoreClient() throws HiveAuthzPluginException {
        try {
            return new AWSCatalogMetastoreClient(Hive.get().getConf(), null);
        } catch (MetaException | HiveException e) {
            throw new HiveAuthzPluginException(e);
        }
    }
}
