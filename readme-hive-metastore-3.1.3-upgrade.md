# Hive Metastore Client 3.1.3 Upgrade

This document describes the changes made to upgrade the AWS Glue Data Catalog Client from Hive 2.3.x to Hive 3.1.3, including the AWS SDK v1 → v2 migration and the Spark integration changes needed to use the upgraded client.

## Summary of Changes

### 1. AWS SDK v1 → v2 Migration

The Glue client library has been migrated from AWS SDK for Java v1 (`com.amazonaws`, 1.12.x) to AWS SDK for Java v2 (`software.amazon.awssdk`, 2.44.x).

**Why:** AWS SDK v2 is required because the Glue Catalog APIs (`getCatalog`, `getCatalogs`, `createCatalog`, `updateCatalog`, `deleteCatalog`) are only available in SDK v2. These APIs are needed for multi-catalog support in Hive 3.

#### Key Changes

| Area | SDK v1 | SDK v2 |
|------|--------|--------|
| Glue client type | `com.amazonaws.services.glue.AWSGlue` | `software.amazon.awssdk.services.glue.GlueClient` |
| Model package | `com.amazonaws.services.glue.model.*` | `software.amazon.awssdk.services.glue.model.*` |
| Exception type | `com.amazonaws.AmazonServiceException` | `software.amazon.awssdk.awscore.exception.AwsServiceException` |
| Request construction | `new XxxRequest().withA(a).withB(b)` | `XxxRequest.builder().a(a).b(b).build()` |
| Response types | `*Result` (e.g., `GetDatabaseResult`) | `*Response` (e.g., `GetDatabaseResponse`) |
| Model getters | `.getName()`, `.getDatabaseList()` | `.name()`, `.databaseList()` |
| Model objects | Mutable (`.setX()`) | Immutable (`.toBuilder().x().build()`) |
| Credentials | `AWSCredentialsProvider` | `AwsCredentialsProvider` |
| Timestamps | `java.util.Date` | `java.time.Instant` |

#### Files Changed

- **POM dependencies**: Replaced `aws-java-sdk-glue` + `aws-java-sdk-core` with `glue`, `sdk-core`, `aws-core`, `sts`, `apache-client`, `arns`
- **Client factory** (`AWSGlueClientFactory`): Rewritten for `GlueClient.builder()` with `Region`, `ClientOverrideConfiguration`, `ApacheHttpClient`
- **Decorator** (`AWSGlueDecoratorBase`): Simplified from full `AWSGlue` interface implementation to composition wrapper
- **Multiple catalog decorator** (`AWSGlueMultipleCatalogDecorator`): Rewritten for immutable SDK v2 objects using `.toBuilder()`
- **Credential providers**: Updated to SDK v2 (`DefaultCredentialsProvider`, `AwsSessionCredentials`, `StaticCredentialsProvider`)
- **Converters**: Updated for `Instant`, `SdkBytes`, builder pattern
- **All source files** (~40): Import replacements, getter renames, builder pattern conversions

### 2. Hive 2.3 → 3.1.3 Interface Upgrade

The spark-client's `AWSCatalogMetastoreClient` has been upgraded from Hive 2.3.x `IMetaStoreClient` to Hive 3.1.3 `IMetaStoreClient`.

**Why:** Hive 3.1.3 introduces the Catalog API (`getCatalog`, `getCatalogs`, etc.) and catalogName overloads for database/table operations, enabling multi-catalog support.

#### Signature Changes

| Method | Hive 2.3 | Hive 3.1.3 |
|--------|----------|------------|
| `isCompatibleWith` | `isCompatibleWith(HiveConf)` | `isCompatibleWith(Configuration)` |
| `isSameConfObj` | `isSameConfObj(HiveConf)` | `isSameConfObj(Configuration)` |
| `addDynamicPartitions` | 4 parameters | Added `long writeId` parameter |
| `createTableWithConstraints` | 3 constraint params | Added 4 more: unique, not-null, default, check |
| `listTableNamesByFilter` | throws `MetaException` | `MetaException` removed from throws |

#### Removed Methods

- **Index methods** (index support dropped in Hive 3): `createIndex`, `alter_index`, `getIndex`, `listIndexes`, `listIndexNames`, `dropIndex`
- **Deprecated overloads**: `getTable(String)`, `tableExists(String)`, `dropTable(String, boolean)`

#### New Methods Added (~100)

Most new Hive 3 methods are implemented as `throw new UnsupportedOperationException()`. The following are fully implemented:

**Catalog operations:**
- `getCatalogs()` — returns catalog names from the Glue Data Catalog via `getCatalogs` API
- `getCatalog(String)` — returns a `Catalog` object from the Glue Data Catalog via `getCatalog` API
- `createCatalog(Catalog)` — creates a catalog in the Glue Data Catalog via `createCatalog` API
- `alterCatalog(String, Catalog)` — updates a catalog in the Glue Data Catalog via `updateCatalog` API
- `dropCatalog(String)` — deletes a catalog from the Glue Data Catalog via `deleteCatalog` API

**CatalogName overloads for database operations:**
- `getDatabase(String catalogName, String dbName)`
- `getDatabases(String catalogName, String dbPattern)`
- `getAllDatabases(String catalogName)`
- `alterDatabase(String catalogName, String databaseName, Database)`
- `dropDatabase(String catalogName, String dbName, boolean, boolean, boolean)`

**CatalogName overloads for table operations:**
- `dropTable(String catName, String dbName, String tableName, boolean, boolean, boolean)`
- `tableExists(String catalogName, String databaseName, String tableName)`
- `getTable(String catalogName, String dbName, String tableName)`
- `getTables(String catalogName, String dbname, String tablePattern)`
- `getTables(String catalogName, String dbname, String tablePattern, TableType)`
- `getAllTables(String catalogName, String dbname)`
- `getTableMeta(String catalogName, String dbPatterns, String tablePatterns, List<String>)`
- `getTableObjectsByName(String catalogName, String dbName, List<String>)`
- `listTableNamesByFilter(String catalogName, String dbName, String filter, int)`

> **Note:** The catalogName overloads currently delegate to the non-catalogName versions (ignoring the catalogName parameter). Full multi-catalog routing requires the `GlueCatalogPlugin` described below.

#### Spark-Hive Shims Updates

The `AwsGlueSparkHiveShims` module was updated for Hive 3 API changes:

| Method | Hive 2.3 | Hive 3.1.3 |
|--------|----------|------------|
| `Warehouse.mkdirs` | `mkdirs(path, true)` | `mkdirs(path)` |
| `Warehouse.deleteDir` | `deleteDir(path, recursive, ifPurge)` | `deleteDir(path, recursive, ifPurge, true)` |
| `Warehouse.getDefaultTablePath` | `getDefaultTablePath(db, tableName)` | `getDefaultTablePath(db, tableName, false)` |
| `MetaStoreUtils.requireCalStats` | `requireCalStats(conf, oldPart, ...)` | `requireCalStats(oldPart, ...)` |
| `MetaStoreUtils.updateTableStatsFast` | `updateTableStatsFast(...)` | `updateTableStatsSlow(...)` |
| `MetaStoreUtils` import | `o.a.h.h.metastore.MetaStoreUtils` | `o.a.h.h.metastore.utils.MetaStoreUtils` |
| Version prefix | `"2.3."` | `"3."` |

### 3. GlueCatalogPlugin — Spark V2 Multi-Catalog Support

A new `GlueCatalogPlugin` class implements Spark's `CatalogPlugin`, `SupportsNamespaces`, and `TableCatalog` interfaces, enabling multi-catalog operations in Spark SQL.

**What it enables:**
```sql
SET CATALOG s3tablescatalog;          -- switch to a different Glue catalog
SHOW CATALOGS;                        -- list child catalogs
SHOW DATABASES;                       -- list databases scoped to current catalog
USE my_database;                      -- set database within current catalog
SHOW TABLES;                          -- list tables scoped to current catalog + database
SET CATALOG spark_catalog;            -- switch back to default
```

**Expected behavior by catalog context:**

| Current catalog | `SHOW CATALOGS` | `SHOW DATABASES` | `USE <db>` + `SHOW TABLES` |
|----------------|-----------------|------------------|---------------------------|
| `spark_catalog` (default) | All top-level Glue catalogs + `spark_catalog` | All databases in the default account catalog | Tables in the specified database (default catalog) |
| Two-level parent (e.g., `s3tablescatalog` with `bucket/*`) | Child catalogs only (e.g., S3 table buckets) | Empty — parent catalogs have children, not databases | N/A — no databases to select |
| Two-level child (e.g., `s3tablescatalog/<bucket-name>`) | Empty — child catalogs have no further children | Databases (namespaces) in that table bucket | Tables in the specified database within that child catalog |
| One-level catalog (e.g., a standalone federated catalog) | Empty — no children | Databases scoped to that catalog | Tables in the specified database within that catalog |

**How it works:**
- Uses the Glue SDK directly (not the Hive `IMetaStoreClient`) to avoid catalog ID format validation issues
- `listNamespaces()` calls `glueClient.getDatabases(catalogId=catalogName)`
- `listTables()` calls `glueClient.getTables(catalogId=catalogName, databaseName=namespace)`
- `listChildCatalogs()` calls `glueClient.getCatalogs(parentCatalogId=catalogName)`
- Auto-discovered by Spark's `CatalogManager` when a catalog name exists in Glue but isn't pre-registered

**Configuration (optional):**
```
spark.sql.catalog.<name>=com.amazonaws.glue.catalog.metastore.GlueCatalogPlugin
```

If not configured, catalogs are auto-discovered from Glue when `SET CATALOG <name>` is executed (requires the Spark-side `CatalogManager` modification described below).



## Required Spark Changes

Apache Spark compiles its `sql/hive` module against Hive 2.3.10 by default (as of Spark 4.x / 3.5.x — see [building-spark.md](https://github.com/apache/spark/blob/master/docs/building-spark.md)). This creates two gaps that prevent Spark from utilizing the upgraded Hive 3.1.3 client:

1. **Binary incompatibility**: Spark's `sql/hive` module references Hive 2.3.x APIs that were removed or changed in Hive 3.1.3 (e.g., `Index` type, `PartitionLocationsResult`, `CommandProcessorFactory.clean`, `Date` vs `Instant` in object inspectors). Dropping in the Hive 3.1.3 client without recompiling Spark causes `NoSuchMethodError`, `NoClassDefFoundError`, and type mismatch errors at runtime.

2. **No catalog plumbing**: Spark's `SHOW CATALOGS` command only returns catalogs registered in its in-memory `CatalogManager` — it never queries the Hive metastore. Similarly, `SET CATALOG <name>` requires a pre-registered `CatalogPlugin` class and has no fallback to discover catalogs from the metastore. This means the Hive 3 Catalog API (`getCatalogs`, `getCatalog`, etc.) is never invoked, even though the client implements it.

To close these gaps, the following Spark source changes are needed:

### Hive Version Bump

| File | Change |
|------|--------|
| `pom.xml` | `hive.version` / `hive23.version`: `2.3.x` → `3.1.3` |
| `sql/hive/pom.xml` | Add `hive-standalone-metastore` dependency |

### Hive 2→3 Compilation Fixes in `sql/hive`

| File | Fix |
|------|-----|
| `HiveShim.scala` | Remove `Index`, `PartitionLocationsResult`, `getPartitionLocations`, `getIndexes` |
| `HiveClientImpl.scala` | Remove `PartitionLocation` import, simplify `getPartitionLocations`, remove `CommandProcessorFactory.clean`, use `SessionStream` instead of `PrintStream` for `SessionState.out`/`err` |
| `HiveInspectors.scala` | Convert Hive 3 `Date`/`Timestamp` types to `java.sql` types |
| `TableReader.scala` | Same date/timestamp conversion |
| `HiveFileFormat.scala` | Wrap `Configuration` in `JobConf` for Hive 3 API |
| `HiveTempPath.scala` | `FileUtils.mkdir` signature: 4-arg → 3-arg |

### SHOW CATALOGS Wiring

Add `listCatalogs()` through the Spark catalog abstraction so that `SHOW CATALOGS` queries the Hive metastore:

| File | Change |
|------|--------|
| `HiveClient.scala` | Add `def listCatalogs(): Seq[String]` |
| `HiveClientImpl.scala` | Implement via `msClient.getCatalogs()` |
| `ExternalCatalog.scala` | Add with default `Seq.empty` |
| `ExternalCatalogWithListener.scala` | Delegate to wrapped catalog |
| `HiveExternalCatalog.scala` | Delegate to `HiveClient` |
| `SessionCatalog.scala` | Delegate to `ExternalCatalog` |
| `CatalogManager.scala` | Merge in-memory catalogs with metastore catalogs; when current catalog has `listChildCatalogs()`, return only child catalogs |

### SET CATALOG Auto-Discovery

Allow `SET CATALOG <name>` to work without pre-registering each catalog via `spark.sql.catalog.<name>`:

| File | Change |
|------|--------|
| `CatalogManager.scala` | In `catalog(name)`, fall back to `GlueCatalogPlugin.existsInGlue()` when `Catalogs.load()` fails with `CatalogNotFoundException` |

> **Note on Hive version**: If using vanilla Hive 3.1.3 (not the `amzn-21` variant), you may encounter a `ClassCastException` (`URLClassLoader` cast in `SessionState`) on Java 17. The `amzn-21` variant includes a fix for this.
