package de.bwaldvogel.mongo.backend.postgresql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.AbstractSynchronizedMongoDatabase;
import de.bwaldvogel.mongo.backend.CollectionOptions;
import de.bwaldvogel.mongo.backend.CursorRegistry;
import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.backend.IndexKey;
import de.bwaldvogel.mongo.backend.postgresql.index.PostgresUniqueIndex;
import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.oplog.Oplog;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class PostgresqlDatabase extends AbstractSynchronizedMongoDatabase<Long> {

    private final PostgresqlBackend backend;

    public PostgresqlDatabase(String databaseName, PostgresqlBackend backend, CursorRegistry cursorRegistry) {
        super(databaseName, cursorRegistry);
        this.backend = backend;
        initializeNamespacesAndIndexes();
    }

    @Override
    public void drop(Oplog oplog) {
        try (Connection connection = backend.getConnection();
             PreparedStatement stmt = connection.prepareStatement("DROP SCHEMA " + getSchemaName() + " CASCADE")
        ) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new MongoServerException("failed to drop " + this, e);
        }
    }

    private String getSchemaName() {
        return getSchemaName(getDatabaseName());
    }

    @Override
    protected long getFileSize() {
        return 0;
    }

    @Override
    protected long getStorageSize() {
        return 0;
    }

    @Override
    protected Index<Long> openOrCreateUniqueIndex(String collectionName, String indexName, List<IndexKey> keys, boolean sparse) {
        PostgresUniqueIndex index = new PostgresUniqueIndex(backend, databaseName, collectionName, indexName, keys, sparse);
        index.initialize();
        return index;
    }

    @Override
    public void dropCollection(String collectionName, Oplog oplog) {
        super.dropCollection(collectionName, oplog);
        String fullCollectionName = PostgresqlCollection.getQualifiedTablename(getDatabaseName(), collectionName);
        try (Connection connection = backend.getConnection();
             PreparedStatement stmt = connection.prepareStatement("DROP TABLE " + fullCollectionName + "")) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new MongoServerException("failed to drop collection " + collectionName, e);
        }
    }

    @Override
    protected MongoCollection<Long> openOrCreateCollection(String collectionName, CollectionOptions options) {
        String tableName = PostgresqlCollection.getTablename(collectionName);
        String fullCollectionName = PostgresqlCollection.getQualifiedTablename(getDatabaseName(), collectionName);
        String createTableSql = "CREATE TABLE IF NOT EXISTS " + fullCollectionName + "" +
            " (id serial," +
            "  data json," +
            " CONSTRAINT \"pk_" + tableName + "\" PRIMARY KEY (id)" +
            ")";
        String insertSql = "INSERT INTO " + getDatabaseName() + "._meta (collection_name, datasize) VALUES (?, 0) ON CONFLICT DO NOTHING";
        try (Connection connection = backend.getConnection();
             PreparedStatement stmt1 = connection.prepareStatement(createTableSql);
             PreparedStatement stmt2 = connection.prepareStatement(insertSql)) {
            stmt1.executeUpdate();
            stmt2.setString(1, collectionName);
            stmt2.executeUpdate();
        } catch (SQLException e) {
            throw new MongoServerException("failed to create or open collection " + collectionName, e);
        }

        return new PostgresqlCollection(this, collectionName, options, cursorRegistry);
    }

    public PostgresqlBackend getBackend() {
        return backend;
    }

    static String getSchemaName(String databaseName) {
        if (!databaseName.matches("^[a-zA-Z0-9_-]+$")) {
            throw new IllegalArgumentException("Illegal database name: " + databaseName);
        }
        return databaseName;
    }

}
