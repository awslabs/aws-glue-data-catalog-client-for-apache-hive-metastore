## AWS Glue Data Catalog Client for Apache Hive Metastore
The AWS Glue Data Catalog is a fully managed, Apache Hive Metastore compatible, metadata repository. Customers can use the Data Catalog as a central repository to store structural and operational metadata for their data.

AWS Glue provides out-of-box integration with Amazon EMR that enables customers to use the AWS Glue Data Catalog as an external Hive Metastore. To learn more, visit our [documentation](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hive-metastore-glue.html).

This is an open-source implementation of the Apache Hive Metastore client on Amazon EMR clusters that uses the AWS Glue Data Catalog as an external Hive Metastore. It serves as a reference implementation for building a Hive Metastore-compatible client that connects to the AWS Glue Data Catalog. It may be ported to other Hive Metastore-compatible platforms such as other Hadoop and Apache Spark distributions.

This package is compatible with Spark 3 and Hive 3.

**Note**: in order for this client implementation to be used with Apache Hive, a patch included in this [JIRA](https://issues.apache.org/jira/browse/HIVE-12679) must be applied to it. All versions of Apache Hive running on Amazon EMR that support the AWS Glue Data Catalog as the metastore already include this patch. Please follow **all steps listed below in the following order**.

## Patching Apache Hive and Installing It Locally

Obtain a copy of Hive from GitHub at https://github.com/apache/hive.

	git clone https://github.com/apache/hive.git

To build the Hive client, you need to first apply this [patch](https://github.com/awslabs/aws-glue-data-catalog-client-for-apache-hive-metastore/blob/branch-3.4.0/branch_3.1.patch).  Download this patch and move it to your local Hive git repository you created above. This patch is included in the repository. Apply the patch and build Hive.

    cd <your local Hive repo>	
    git checkout branch-3.1
	git apply -3 branch_3.1.patch
	mvn clean install -DskipTests

As Spark uses a fork of Hive based off the 2.3 branch, in order to build the Spark client, you need Hive 2.3 built with this [patch](https://issues.apache.org/jira/secure/attachment/12958418/HIVE-12679.branch-2.3.patch).

If building off the previous Hive repo, please reset those changes:

    git add .
    git reset --hard

Continue with patching the 2.3 branch:

	cd <your local Hive repo>
	git checkout branch-2.3
    patch -p0 <HIVE-12679.branch-2.3.patch
    mvn clean install -DskipTests

## Building the Glue Data Catalog Client

When building for the first time, all clients must be built from the root directory of the AWS Glue Data Catalog Client repository. This will build both the Hive and Spark clients and necessary dependencies.

    cd aws-glue-data-catalog-client-for-apache-hive-metastore
    mvn clean install -DskipTests

Once this is done, the individual clients are free to be built separately:

To build the Spark client:

    cd aws-glue-datacatalog-spark-client
    mvn clean package -DskipTests

To build the Hive client:

    cd aws-glue-datacatalog-hive3-client
    mvn clean package -DskipTests

## Configuring Hive to Use the Hive Client

You need to ensure that the AWS Glue Data Catalog Client jar is in Hive's CLASSPATH and also set the "hive.metastore.client.factory.class" HiveConf variable for Hive to pick up and instantiate the AWS Glue Data Catalog Client.  For instance, on Amazon EMR, the client jar is located in /usr/lib/hive/lib/ and the HiveConf is set in /usr/lib/hive/conf/hive-site.xml.

	<property>
 		<name>hive.metastore.client.factory.class</name>
 		<value>com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory</value>
	</property>

## Configuring Spark to Use the Spark Client

Similarly, for Spark, you need to install the client jar in Spark's CLASSPATH and create or update Spark's own hive-site.xml to add the above property.  On Amazon EMR, this is set in /usr/lib/spark/conf/hive-site.xml.  You can also find the location of the Spark client jar in /usr/lib/spark/conf/spark-defaults.conf.

## Enabling client side caching for catalog

Currently, we provide support for caching:

a) Table metadata - Response from Glue's GetTable operation (https://docs.aws.amazon.com/glue/latest/webapi/API_GetTable.html#API_GetTable_ResponseSyntax)
b) Database metadata - Response from Glue's GetDatabase operation (https://docs.aws.amazon.com/glue/latest/webapi/API_GetDatabase.html#API_GetDatabase_ResponseSyntax)

Both these entities have dedicated caches for themselves and can be enabled/tuned individually.

To enable/tune Table cache, use the following properties in your hive/spark configuration file:

	<property>
 		<name>aws.glue.cache.table.enable</name>
 		<value>true</value>
	</property>
	<property>
 		<name>aws.glue.cache.table.size</name>
 		<value>1000</value>
	</property>
	<property>
 		<name>aws.glue.cache.table.ttl-mins</name>
 		<value>30</value>
	</property>

To enable/tune Database cache:

	<property>
 		<name>aws.glue.cache.db.enable</name>
 		<value>true</value>
	</property>
	<property>
 		<name>aws.glue.cache.db.size</name>
 		<value>1000</value>
	</property>
	<property>
 		<name>aws.glue.cache.db.ttl-mins</name>
 		<value>30</value>
	</property>

NOTE: The caching logic is disabled by default.

## License

This library is licensed under the Apache 2.0 License. 