package com.amazonaws.glue.catalog.metastore;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;

public class AWSGlueDataCatalogHiveClientFactory implements HiveMetastoreClientFactory {

  @Override
  public IMetaStoreClient getHiveMetastoreClient() {
    AWSCatalogMetastoreClient client = null;
    try {
      client = new AWSCatalogMetastoreClient(Hive.get().getConf());
    } catch (HiveException | MetaException e) {
      e.printStackTrace();
    }
    return client;
  }


}
