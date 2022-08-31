package com.amazonaws.glue.catalog.metastore;

import com.amazonaws.services.glue.AWSGlue;
import org.apache.hadoop.hive.metastore.api.MetaException;

/***
 * Interface for creating Glue AWS Client
 */
public interface GlueClientFactory {

  AWSGlue newClient() throws MetaException;

}
