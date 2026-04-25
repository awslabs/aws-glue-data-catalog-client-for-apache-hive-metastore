package com.amazonaws.glue.catalog.metastore;

import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Catalog;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.GetCatalogRequest;
import software.amazon.awssdk.services.glue.model.GetCatalogsRequest;
import software.amazon.awssdk.services.glue.model.GetCatalogsResponse;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A Spark V2 CatalogPlugin backed by AWS Glue Data Catalog.
 * Uses the Glue SDK directly — no Hive IMetaStoreClient needed.
 * Supports SHOW CATALOGS, SET CATALOG, SHOW DATABASES, USE, and SHOW TABLES.
 */
public class GlueCatalogPlugin implements CatalogPlugin, SupportsNamespaces, TableCatalog {

    private String catalogName;
    private GlueClient glueClient;

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
        this.catalogName = name;
        String region = options.getOrDefault("glue.region",
                System.getenv("AWS_REGION") != null ? System.getenv("AWS_REGION") : "us-east-1");
        this.glueClient = GlueClient.builder().region(Region.of(region)).build();
    }

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public String[] defaultNamespace() {
        return new String[0];
    }

    // ==================== SupportsNamespaces ====================

    @Override
    public String[][] listNamespaces() throws NoSuchNamespaceException {
        try {
            List<String> dbs = new ArrayList<>();
            String nextToken = null;
            do {
                GetDatabasesRequest.Builder b = GetDatabasesRequest.builder()
                        .catalogId(catalogName);
                if (nextToken != null) b.nextToken(nextToken);
                GetDatabasesResponse resp = glueClient.getDatabases(b.build());
                for (Database db : resp.databaseList()) {
                    dbs.add(db.name());
                }
                nextToken = resp.nextToken();
            } while (nextToken != null);
            return dbs.stream().map(db -> new String[]{db}).toArray(String[][]::new);
        } catch (Exception e) {
            return new String[0][];
        }
    }

    @Override
    public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
        return new String[0][];
    }

    @Override
    public boolean namespaceExists(String[] namespace) {
        if (namespace.length != 1) return false;
        try {
            glueClient.getDatabase(GetDatabaseRequest.builder()
                    .catalogId(catalogName).name(namespace[0]).build());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(String[] namespace) throws NoSuchNamespaceException {
        if (namespace.length != 1) throw new NoSuchNamespaceException(namespace);
        try {
            Database db = glueClient.getDatabase(GetDatabaseRequest.builder()
                    .catalogId(catalogName).name(namespace[0]).build()).database();
            Map<String, String> meta = new HashMap<>();
            meta.put("comment", db.description() != null ? db.description() : "");
            meta.put("location", db.locationUri() != null ? db.locationUri() : "");
            if (db.hasParameters()) meta.putAll(db.parameters());
            return meta;
        } catch (Exception e) {
            throw new NoSuchNamespaceException(namespace);
        }
    }

    @Override
    public void createNamespace(String[] namespace, Map<String, String> metadata) {
        throw new UnsupportedOperationException("createNamespace is not supported");
    }

    @Override
    public void alterNamespace(String[] namespace, NamespaceChange... changes) {
        throw new UnsupportedOperationException("alterNamespace is not supported");
    }

    @Override
    public boolean dropNamespace(String[] namespace, boolean cascade) {
        throw new UnsupportedOperationException("dropNamespace is not supported");
    }

    // ==================== TableCatalog ====================

    @Override
    public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
        if (namespace.length != 1) throw new NoSuchNamespaceException(namespace);
        try {
            List<Identifier> tables = new ArrayList<>();
            String nextToken = null;
            do {
                GetTablesRequest.Builder b = GetTablesRequest.builder()
                        .catalogId(catalogName).databaseName(namespace[0]);
                if (nextToken != null) b.nextToken(nextToken);
                GetTablesResponse resp = glueClient.getTables(b.build());
                for (software.amazon.awssdk.services.glue.model.Table t : resp.tableList()) {
                    tables.add(Identifier.of(namespace, t.name()));
                }
                nextToken = resp.nextToken();
            } while (nextToken != null);
            return tables.toArray(new Identifier[0]);
        } catch (Exception e) {
            throw new NoSuchNamespaceException(namespace);
        }
    }

    @Override
    public Table loadTable(Identifier ident) throws NoSuchTableException {
        throw new NoSuchTableException(ident);
    }

    @Override
    public Table createTable(Identifier ident, StructType schema, Transform[] partitions,
                             Map<String, String> properties) throws TableAlreadyExistsException {
        throw new UnsupportedOperationException("createTable is not supported");
    }

    @Override
    public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
        throw new UnsupportedOperationException("alterTable is not supported");
    }

    @Override
    public boolean dropTable(Identifier ident) {
        throw new UnsupportedOperationException("dropTable is not supported");
    }

    @Override
    public void renameTable(Identifier oldIdent, Identifier newIdent) throws NoSuchTableException {
        throw new UnsupportedOperationException("renameTable is not supported");
    }

    // ==================== Glue Catalog Operations ====================

    /** Lists child catalogs under this catalog. */
    public List<String> listChildCatalogs() {
        try {
            List<String> result = new ArrayList<>();
            String nextToken = null;
            do {
                GetCatalogsRequest.Builder b = GetCatalogsRequest.builder()
                        .parentCatalogId(catalogName);
                if (nextToken != null) b.nextToken(nextToken);
                GetCatalogsResponse resp = glueClient.getCatalogs(b.build());
                for (Catalog cat : resp.catalogList()) {
                    result.add(cat.name());
                }
                nextToken = resp.nextToken();
            } while (nextToken != null);
            return result;
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    /** Checks if this catalog exists in Glue. */
    public static boolean existsInGlue(String catalogName, String region) {
        try {
            GlueClient client = GlueClient.builder().region(Region.of(region)).build();
            client.getCatalog(GetCatalogRequest.builder().catalogId(catalogName).build());
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
