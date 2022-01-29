## AWS Glue Data Catalog Client for Apache Hive Metastore
The AWS Glue Data Catalog is a fully managed, Apache Hive Metastore compatible, metadata repository. Customers can use the Data Catalog as a central repository to store structural and operational metadata for their data.

AWS Glue provides out-of-box integration with Amazon EMR that enables customers to use the AWS Glue Data Catalog as an external Hive Metastore. To learn more, visit our [documentation](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hive-metastore-glue.html).

This is an open-source implementation of the Apache Hive Metastore client on Amazon EMR clusters that uses the AWS Glue Data Catalog as an external Hive Metastore. It serves as a reference implementation for building a Hive Metastore-compatible client that connects to the AWS Glue Data Catalog. It may be ported to other Hive Metastore-compatible platforms such as other Hadoop and Apache Spark distributions.

This package is compatible with Spark 3 and Hive 3

**Note**: in order for this client implementation to be used with Apache Hive, a patch included in this [JIRA](https://issues.apache.org/jira/browse/HIVE-12679) must be applied to it. All versions of Apache Hive running on Amazon EMR that support the AWS Glue Data Catalog as the metastore already include this patch.

## Patching Apache Hive and Installing It Locally

Obtain a copy of Hive from GitHub at https://github.com/apache/hive.

	git clone https://github.com/apache/hive.git

To build the Hive client, you need to first apply this [patch](https://github.com/awslabs/aws-glue-data-catalog-client-for-apache-hive-metastore/blob/Updated_connector/branch_3.1.patch).  Download this patch and move it to your local Hive git repository you created above.  Apply the patch and build Hive.

	git checkout branch-3.1
	git apply -3 ~/branch_3.1.patch
	mvn clean install -DskipTests

## Building the Hive Client

Once you have successfully patched and installed Hive locally, move into the AWS Glue Data Catalog Client repository and update the following property in pom.xml.

	<hive3.version>3.1.3</hive3.version>

You are now ready to build the Hive client.

    cd aws-glue-datacatalog-hive2-client
    mvn clean package -DskipTests


## Building the Spark Client

As Spark uses a fork of Hive based off the 2.3 branch, in order to build the Spark client, you need Hive 2.3 built with this [patch](https://issues.apache.org/jira/secure/attachment/12958418/HIVE-12679.branch-2.3.patch).

	cd <your local Hive repo>
	git checkout branch-2.3
    patch -p0 <HIVE-12679.branch-2.3.patch
    mvn clean install -DskipTests


Go back to the AWS Glue Data Catalog Client repository and update the following property in pom.xml to match the version of Hive you just patched and installed locally.

	<spark-hive.version>2.3.10-SNAPSHOT</spark-hive.version>

You are now ready to build the Spark client.

    cd aws-glue-datacatalog-spark-client
    mvn clean package -DskipTests

If you are having issues with building individual folders and if you have both versions of Hive patched and installed locally, you can build both of these clients from the root directory of the AWS Glue Data Catalog Client repository.

## Configuring Hive to Use the Hive Client

You need to ensure that the AWS Glue Data Catalog Client jar is in Hive's CLASSPATH and also set the "hive.metastore.client.factory.class" HiveConf variable for Hive to pick up and instantiate the AWS Glue Data Catalog Client.  For instance, on Amazon EMR, the client jar is located in /usr/lib/hive/lib/ and the HiveConf is set in /usr/lib/hive/conf/hive-site.xml.

	<property>
 		<name>hive.metastore.client.factory.class</name>
 		<value>com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory</value>
	</property>

## Configuring Spark to Use the Spark Client

Similarly, for Spark, you need to install the client jar in Spark's CLASSPATH and create or update Spark's own hive-site.xml to add the above property.  On Amazon EMR, this is set in /usr/lib/spark/conf/hive-site.xml.  You can also find the location of the Spark client jar in /usr/lib/spark/conf/spark-defaults.conf.

## License

This library is licensed under the Apache 2.0 License. 