## AWS Glue Data Catalog Client for Apache Hive Metastore
The AWS Glue Data Catalog is a fully managed, Apache Hive Metastore compatible, metadata repository. Customers can use the Data Catalog as a central repository to store structural and operational metadata for their data.

AWS Glue provides out-of-box integration with Amazon EMR that enables customers to use the AWS Glue Data Catalog as an external Hive Metastore. To learn more, visit our [documentation](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hive-metastore-glue.html).

This is an open-source implementation of the Apache Hive Metastore client on Amazon EMR clusters that uses the AWS Glue Data Catalog as an external Hive Metastore. It serves as a reference implementation for building a Hive Metastore-compatible client that connects to the AWS Glue Data Catalog. It may be ported to other Hive Metastore-compatible platforms such as other Hadoop and Apache Spark distributions.

## License

This library is licensed under the Apache 2.0 License. 
