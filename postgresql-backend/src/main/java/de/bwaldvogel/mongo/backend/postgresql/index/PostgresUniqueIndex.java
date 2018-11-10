package de.bwaldvogel.mongo.backend.postgresql.index;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.backend.postgresql.PostgresqlBackend;
import de.bwaldvogel.mongo.backend.postgresql.PostgresqlCollection;
import de.bwaldvogel.mongo.backend.postgresql.PostgresqlUtils;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.Missing;
import de.bwaldvogel.mongo.exception.DuplicateKeyError;
import de.bwaldvogel.mongo.exception.KeyConstraintError;
import de.bwaldvogel.mongo.exception.MongoServerException;

public class PostgresUniqueIndex extends Index<Long> {

    private final PostgresqlBackend backend;
    private final String fullCollectionName;

    public PostgresUniqueIndex(PostgresqlBackend backend, String databaseName, String collectionName, String key, boolean ascending) {
        super(key, ascending);
        this.backend = backend;
        fullCollectionName = PostgresqlCollection.getQualifiedTablename(databaseName, collectionName);
        String indexName = collectionName + "_" + key;
        String sql = "CREATE UNIQUE INDEX IF NOT EXISTS \"" + indexName + "\" ON " + fullCollectionName + " ((" + PostgresqlUtils.toDataKey(key) + "))";
        try (Connection connection = backend.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new MongoServerException("failed to create unique index on " + fullCollectionName, e);
        }
    }

    @Override
    public void checkAdd(Document document) {
        Object keyValue = Utils.getSubdocumentValue(document, key);
        if (keyValue instanceof Missing) {
            keyValue = null;
        }
        String sql = createSelectStatement(keyValue);
        try (Connection connection = backend.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            if (keyValue != null) {
                stmt.setString(1, PostgresqlUtils.toQueryValue(keyValue));
            }
            try (ResultSet resultSet = stmt.executeQuery()) {
                if (resultSet.next()) {
                    throw new DuplicateKeyError(this, keyValue);
                }
            }
        } catch (SQLException | IOException e) {
            throw new MongoServerException("failed to remove document from " + fullCollectionName, e);
        }
    }

    @Override
    public void add(Document document, Long position) {
    }

    @Override
    public Long remove(Document document) {
        Object keyValue = Utils.getSubdocumentValue(document, key);
        String sql = createSelectStatement(keyValue);
        try (Connection connection = backend.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            if (keyValue != null) {
                stmt.setString(1, PostgresqlUtils.toQueryValue(keyValue));
            }
            try (ResultSet resultSet = stmt.executeQuery()) {
                if (!resultSet.next()) {
                    return null;
                }
                long position = resultSet.getLong("id");
                if (resultSet.next()) {
                    throw new MongoServerException("got more than one id");
                }
                return Long.valueOf(position);
            }
        } catch (SQLException | IOException e) {
            throw new MongoServerException("failed to remove document from " + fullCollectionName, e);
        }
    }

    private String createSelectStatement(Object keyValue) {
        return "SELECT id FROM " + fullCollectionName + " WHERE " + PostgresqlUtils.toDataKey(key) + (keyValue == null ? " IS NULL" : " = ?");
    }

    @Override
    public boolean canHandle(Document query) {
        return false;
    }

    @Override
    public Iterable<Long> getPositions(Document query) {
        return null;
    }

    @Override
    public long getCount() {
        return 0;
    }

    @Override
    public long getDataSize() {
        return 0;
    }

    @Override
    public void checkUpdate(Document oldDocument, Document newDocument) {
    }

    @Override
    public void updateInPlace(Document oldDocument, Document newDocument) throws KeyConstraintError {
    }
}
