package de.bwaldvogel.mongo.backend.postgresql.index;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.backend.Assert;
import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.backend.IndexKey;
import de.bwaldvogel.mongo.backend.KeyValue;
import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.backend.postgresql.PostgresqlBackend;
import de.bwaldvogel.mongo.backend.postgresql.PostgresqlCollection;
import de.bwaldvogel.mongo.backend.postgresql.PostgresqlUtils;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.DuplicateKeyError;
import de.bwaldvogel.mongo.exception.KeyConstraintError;
import de.bwaldvogel.mongo.exception.MongoServerException;

public class PostgresUniqueIndex extends Index<Long> {

    private final PostgresqlBackend backend;
    private final String fullCollectionName;
    private final String indexName;

    public PostgresUniqueIndex(PostgresqlBackend backend, String databaseName, String collectionName, List<IndexKey> keys, boolean sparse) {
        super(keys, sparse);
        this.backend = backend;
        fullCollectionName = PostgresqlCollection.getQualifiedTablename(databaseName, collectionName);
        indexName = collectionName + "_" + indexName(keys);
    }

    public void initialize() {
        String columns = keyColumns(getKeys());
        String sql = "CREATE UNIQUE INDEX IF NOT EXISTS \"" + indexName + "\" ON " + fullCollectionName + " (" + columns + ")";
        try (Connection connection = backend.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new MongoServerException("failed to create unique index on " + fullCollectionName, e);
        }
    }

    private static String keyColumns(List<IndexKey> keys) {
        return keys.stream()
            .map(key -> "(" + PostgresqlUtils.toDataKey(key.getKey()) + ") " + (key.isAscending() ? "ASC" : "DESC"))
            .collect(Collectors.joining(", "));
    }

    private static String indexName(List<IndexKey> keys) {
        Assert.notEmpty(keys, () -> "No keys");
        return keys.stream()
            .map(k -> k.getKey() + "_" + (k.isAscending() ? "ASC" : "DESC"))
            .collect(Collectors.joining("_"));
    }

    @Override
    public void checkAdd(Document document, MongoCollection<Long> collection) {
        checkDocument(document, collection, "add");
    }

    private void checkDocument(Document document, MongoCollection<Long> collection, String operation) {
        Map<String, Object> keyValues = getKeyValueMap(document);
        String sql = createSelectStatement(keyValues);
        try (Connection connection = backend.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            fillStrings(stmt, keyValues);
            try (ResultSet resultSet = stmt.executeQuery()) {
                if (resultSet.next()) {
                    throw new DuplicateKeyError(this, collection, new KeyValue(keyValues.values()));
                }
            }
        } catch (SQLException | IOException e) {
            throw new MongoServerException("failed to " + operation + " document to " + fullCollectionName, e);
        }
    }

    private Map<String, Object> getKeyValueMap(Document document) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (String key : keys()) {
            Object value = Utils.getSubdocumentValueCollectionAware(document, key);
            if (value instanceof Missing) {
                value = null;
            }
            result.put(key, value);
        }
        return result;
    }

    @Override
    public void add(Document document, Long position, MongoCollection<Long> collection) {
    }

    @Override
    public Long remove(Document document) {
        Map<String, Object> keyValues = getKeyValueMap(document);
        String sql = createSelectStatement(keyValues);
        try (Connection connection = backend.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            fillStrings(stmt, keyValues);
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

    private void fillStrings(PreparedStatement statement, Map<String, Object> keyValues) throws SQLException, IOException {
        int pos = 1;
        for (Object keyValue : keyValues.values()) {
            if (keyValue != null) {
                String queryValue = PostgresqlUtils.toQueryValue(keyValue);
                statement.setString(pos++, queryValue);
            } else if (isSparse()) {
                statement.setNull(pos++, Types.VARCHAR);
            }
        }
    }

    String createSelectStatement(Map<String, Object> keyValues) {
        return "SELECT id FROM " + fullCollectionName + " WHERE " +
            keyValues.entrySet().stream()
                .map(entry -> {
                    String dataKey = PostgresqlUtils.toDataKey(entry.getKey());
                    if (!isSparse() && entry.getValue() == null) {
                        return dataKey + " IS NULL";
                    } else if (entry.getValue() instanceof Number) {
                        return "CASE json_typeof(" + dataKey.replace("->>", "->") + ")"
                            + " WHEN 'number' THEN (" + dataKey + ")::numeric = ?::numeric"
                            + " ELSE FALSE"
                            + " END";
                    } else {
                        return dataKey + " = ?";
                    }
                })
                .collect(Collectors.joining(" AND "));
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
    public void checkUpdate(Document oldDocument, Document newDocument, MongoCollection<Long> collection) {
        if (!nullAwareEqualsKeys(oldDocument, newDocument)) {
            checkDocument(newDocument, collection, "update");
        }
    }

    @Override
    public void updateInPlace(Document oldDocument, Document newDocument, MongoCollection<Long> collection) throws KeyConstraintError {
    }
}
