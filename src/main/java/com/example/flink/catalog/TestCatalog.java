package com.example.flink.catalog;

import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class TestCatalog implements Catalog {

    private final String catalogName;
    private final String defaultDatabase;
    private final Map<String, CatalogDatabase> databases;
    private final Map<ObjectPath, CatalogTable> tables;
    private final Map<ObjectPath, CatalogFunction> functions;

    public TestCatalog(String catalogName, String defaultDatabase) {
        this.catalogName = catalogName;
        this.defaultDatabase = defaultDatabase;
        this.databases = new ConcurrentHashMap<>();
        this.tables = new ConcurrentHashMap<>();
        this.functions = new ConcurrentHashMap<>();

        this.databases.put(defaultDatabase, new CatalogDatabaseImpl(
            Collections.emptyMap(),
            "Default test database"
        ));
    }

    @Override
    public void open() throws CatalogException {
    }

    @Override
    public void close() throws CatalogException {
    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return defaultDatabase;
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return new ArrayList<>(databases.keySet());
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (!databases.containsKey(databaseName)) {
            throw new DatabaseNotExistException(catalogName, databaseName);
        }
        return databases.get(databaseName);
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return databases.containsKey(databaseName);
    }

    @Override
    public void createDatabase(String databaseName, CatalogDatabase database, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        if (databases.containsKey(databaseName)) {
            if (!ignoreIfExists) {
                throw new DatabaseAlreadyExistException(catalogName, databaseName);
            }
        } else {
            databases.put(databaseName, database);
        }
    }

    @Override
    public void dropDatabase(String databaseName, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        if (!databases.containsKey(databaseName)) {
            if (!ignoreIfNotExists) {
                throw new DatabaseNotExistException(catalogName, databaseName);
            }
            return;
        }

        if (!cascade) {
            List<String> tablesInDb = listTables(databaseName);
            if (!tablesInDb.isEmpty()) {
                throw new DatabaseNotEmptyException(catalogName, databaseName);
            }
        }

        databases.remove(databaseName);
        tables.entrySet().removeIf(entry -> entry.getKey().getDatabaseName().equals(databaseName));
    }

    @Override
    public void alterDatabase(String databaseName, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        if (!databases.containsKey(databaseName)) {
            if (!ignoreIfNotExists) {
                throw new DatabaseNotExistException(catalogName, databaseName);
            }
        } else {
            databases.put(databaseName, newDatabase);
        }
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(catalogName, databaseName);
        }

        return tables.keySet().stream()
            .filter(objectPath -> objectPath.getDatabaseName().equals(databaseName))
            .map(ObjectPath::getObjectName)
            .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
    }

    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        if (!tables.containsKey(tablePath)) {
            throw new TableNotExistException(catalogName, tablePath);
        }
        return tables.get(tablePath);
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        return tables.containsKey(tablePath);
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (!tables.containsKey(tablePath)) {
            if (!ignoreIfNotExists) {
                throw new TableNotExistException(catalogName, tablePath);
            }
        } else {
            tables.remove(tablePath);
        }
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException, CatalogException {
        if (!tables.containsKey(tablePath)) {
            if (!ignoreIfNotExists) {
                throw new TableNotExistException(catalogName, tablePath);
            }
            return;
        }

        ObjectPath newPath = new ObjectPath(tablePath.getDatabaseName(), newTableName);
        if (tables.containsKey(newPath)) {
            throw new TableAlreadyExistException(catalogName, newPath);
        }

        CatalogTable table = tables.remove(tablePath);
        tables.put(newPath, table);
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(catalogName, tablePath.getDatabaseName());
        }

        if (tables.containsKey(tablePath)) {
            if (!ignoreIfExists) {
                throw new TableAlreadyExistException(catalogName, tablePath);
            }
        } else {
            tables.put(tablePath, (CatalogTable) table);
        }
    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (!tables.containsKey(tablePath)) {
            if (!ignoreIfNotExists) {
                throw new TableNotExistException(catalogName, tablePath);
            }
        } else {
            tables.put(tablePath, (CatalogTable) newTable);
        }
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> filters)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        throw new PartitionNotExistException(catalogName, tablePath, partitionSpec);
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        return false;
    }

    @Override
    public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition partition, boolean ignoreIfExists)
            throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionAlreadyExistsException, CatalogException {
        throw new UnsupportedOperationException("Partitions not supported in TestCatalog");
    }

    @Override
    public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("Partitions not supported in TestCatalog");
    }

    @Override
    public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition newPartition, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("Partitions not supported in TestCatalog");
    }

    @Override
    public List<String> listFunctions(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(catalogName, databaseName);
        }
        return functions.keySet().stream()
            .filter(objectPath -> objectPath.getDatabaseName().equals(databaseName))
            .map(ObjectPath::getObjectName)
            .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {
        if (!functions.containsKey(functionPath)) {
            throw new FunctionNotExistException(catalogName, functionPath);
        }
        return functions.get(functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return functions.containsKey(functionPath);
    }

    @Override
    public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
            throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        if (!databaseExists(functionPath.getDatabaseName())) {
            throw new DatabaseNotExistException(catalogName, functionPath.getDatabaseName());
        }

        if (functions.containsKey(functionPath)) {
            if (!ignoreIfExists) {
                throw new FunctionAlreadyExistException(catalogName, functionPath);
            }
        } else {
            functions.put(functionPath, function);
        }
    }

    @Override
    public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        if (!functions.containsKey(functionPath)) {
            if (!ignoreIfNotExists) {
                throw new FunctionNotExistException(catalogName, functionPath);
            }
        } else {
            functions.put(functionPath, newFunction);
        }
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        if (!functions.containsKey(functionPath)) {
            if (!ignoreIfNotExists) {
                throw new FunctionNotExistException(catalogName, functionPath);
            }
        } else {
            functions.remove(functionPath);
        }
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
    }

    @Override
    public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
    }

    @Override
    public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogTableStatistics partitionStatistics, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
    }

    @Override
    public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.empty();
    }
}