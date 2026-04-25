package com.amazonaws.glue.catalog.metastore;

import software.amazon.awssdk.services.glue.GlueClient;
import org.apache.hadoop.hive.metastore.api.MetaException;

/***
 * Interface for creating Glue AWS Client
 */
public interface GlueClientFactory {

  GlueClient newClient() throws MetaException;

}
