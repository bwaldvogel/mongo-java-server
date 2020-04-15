package de.bwaldvogel.mongo.backend.postgresql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.AbstractMongoCollection;
import de.bwaldvogel.mongo.backend.DocumentWithPosition;
import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.DuplicateKeyError;
import de.bwaldvogel.mongo.exception.MongoServerException;

public class PostgresqlCollection extends AbstractMongoCollection<Long> {

    private final PostgresqlBackend backend;

    public PostgresqlCollection(PostgresqlDatabase database, String collectionName, String idField) {
        super(database, collectionName, idField);
        this.backend = database.getBackend();
    }

    @Override
    public int count() {
        try (Connection connection = backend.getConnection();
             PreparedStatement stmt = connection.prepareStatement("SELECT COUNT(*) FROM " + getQualifiedTablename())
        ) {
            try (ResultSet resultSet = stmt.executeQuery()) {
                if (!resultSet.next()) {
                    throw new MongoServerException("got no result");
                }
                int count = resultSet.getInt(1);
                if (resultSet.next()) {
                    throw new MongoServerException("got more than one result");
                }
                return count;
            }
        } catch (SQLException e) {
            throw new MongoServerException("failed to count " + this, e);
        }
    }

    @Override
    protected QueryResult matchDocuments(Document query, Document orderBy, int numberToSkip, int numberToReturn) {
        List<Document> matchedDocuments = new ArrayList<>();

        String sql = "SELECT data FROM " + getQualifiedTablename() + " " + convertOrderByToSql(orderBy);
        try (Connection connection = backend.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)
        ) {
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    String data = resultSet.getString("data");
                    Document document = JsonConverter.fromJson(data);
                    if (documentMatchesQuery(document, query)) {
                        matchedDocuments.add(document);
                    }
                }
            } catch (IOException e) {
                throw new MongoServerException("failed to parse document", e);
            }
        } catch (SQLException e) {
            throw new MongoServerException("failed to query " + this, e);
        }

        return createQueryResult(matchedDocuments, numberToSkip, numberToReturn);
    }

    static String convertOrderByToSql(Document orderBy) {
        StringBuilder orderBySql = new StringBuilder();
        if (orderBy != null && !orderBy.isEmpty()) {
            orderBySql.append("ORDER BY");
            int num = 0;
            for (String key : orderBy.keySet()) {
                if (num > 0) {
                    orderBySql.append(",");
                }
                int sortValue = getSortValue(orderBy, key);
                orderBySql.append(" ");
                if (key.equals("$natural")) {
                    orderBySql.append("id");
                } else {
                    orderBySql.append(PostgresqlUtils.toNormalizedDataKey(key));
                }
                if (sortValue == 1) {
                    orderBySql.append(" ASC NULLS FIRST");
                } else if (sortValue == -1) {
                    orderBySql.append(" DESC NULLS LAST");
                } else {
                    throw new IllegalArgumentException("Illegal sort value: " + sortValue);
                }
                num++;
            }
        }
        return orderBySql.toString();
    }

    private static int getSortValue(Document orderBy, String key) {
        Object orderByValue = orderBy.get(key);
        try {
            return ((Integer) orderByValue).intValue();
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("Illegal sort value: " + orderByValue);
        }
    }

    @Override
    protected QueryResult matchDocuments(Document query, Iterable<Long> positions, Document orderBy, int numberToSkip, int numberToReturn) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    protected Document getDocument(Long position) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    protected void updateDataSize(int sizeDelta) {
        try (Connection connection = backend.getConnection();
             PreparedStatement stmt = connection.prepareStatement("UPDATE " + getDatabaseName() + "._meta" +
                 " SET datasize = datasize + ? WHERE collection_name = ?")
        ) {
            stmt.setLong(1, sizeDelta);
            stmt.setString(2, getCollectionName());
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new MongoServerException("failed to update datasize", e);
        }
    }

    @Override
    protected int getDataSize() {
        try (Connection connection = backend.getConnection();
             PreparedStatement stmt = connection.prepareStatement("SELECT datasize FROM " + getDatabaseName() + "._meta" +
                 " WHERE collection_name = ?")
        ) {
            stmt.setString(1, getCollectionName());
            return Math.toIntExact(querySingleValue(stmt));
        } catch (SQLException e) {
            throw new MongoServerException("failed to retrieve datasize", e);
        }
    }

    private long querySingleValue(PreparedStatement stmt) throws SQLException, MongoServerException {
        try (ResultSet resultSet = stmt.executeQuery()) {
            if (!resultSet.next()) {
                throw new MongoServerException("got no value");
            }
            long value = resultSet.getLong(1);
            if (resultSet.next()) {
                throw new MongoServerException("got more than one value");
            }
            return Long.valueOf(value);
        }
    }

    @Override
    protected Long addDocumentInternal(Document document) {
        try (Connection connection = backend.getConnection();
             PreparedStatement stmt = connection.prepareStatement("INSERT INTO " + getQualifiedTablename() +
                 " (data) VALUES (?::json)" +
                 " RETURNING ID")
        ) {
            String documentAsJson = JsonConverter.toJson(document);
            stmt.setString(1, documentAsJson);
            return querySingleValue(stmt);
        } catch (SQLException e) {
            if (PostgresqlUtils.isErrorDuplicateKey(e)) {
                throw new DuplicateKeyError(getDatabaseName() + "." + getCollectionName(), e.getMessage());
            }
            throw new MongoServerException("failed to insert " + document, e);
        }
    }

    private String getQualifiedTablename() {
        return getQualifiedTablename(getDatabaseName(), getCollectionName());
    }

    public static String getQualifiedTablename(String databaseName, String collectionName) {
        return "\"" + PostgresqlDatabase.getSchemaName(databaseName) + "\".\"" + getTablename(collectionName) + "\"";
    }

    static String getTablename(String collectionName) {
        if (!collectionName.matches("^[a-zA-Z0-9_.-]+$")) {
            throw new IllegalArgumentException("Illegal database name: " + collectionName);
        }
        return collectionName.replaceAll("\\.", "_");
    }

    @Override
    protected void removeDocument(Long position) {
        try (Connection connection = backend.getConnection();
             PreparedStatement stmt = connection.prepareStatement("DELETE FROM " + getQualifiedTablename() + " WHERE id = ?")) {
            stmt.setLong(1, position);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new MongoServerException("failed to remove document from " + this, e);
        }
    }

    @Override
    protected Long findDocumentPosition(Document document) {
        if (idField == null || !document.containsKey(idField)) {
            return super.findDocumentPosition(document);
        }
        String sql = "SELECT id FROM " + getQualifiedTablename() + " WHERE " + PostgresqlUtils.toDataKey(idField) + " = ?";
        try (Connection connection = backend.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, PostgresqlUtils.toQueryValue(document.get(idField)));
            try (ResultSet resultSet = stmt.executeQuery()) {
                if (!resultSet.next()) {
                    return null;
                }
                long id = resultSet.getLong(1);
                if (resultSet.next()) {
                    throw new MongoServerException("got more than one id");
                }
                return Long.valueOf(id);
            }
        } catch (SQLException e) {
            throw new MongoServerException("failed to find document position of " + document, e);
        }
    }

    @Override
    protected Stream<DocumentWithPosition<Long>> streamAllDocumentsWithPosition() {
        String sql = "SELECT id, data FROM " + getQualifiedTablename();

        List<DocumentWithPosition<Long>> allDocumentsWithPositions = new ArrayList<>();
        try (Connection connection = backend.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    long id = resultSet.getLong("id");
                    String data = resultSet.getString("data");
                    Document document = JsonConverter.fromJson(data);
                    allDocumentsWithPositions.add(new DocumentWithPosition<>(document, id));
                }
            }
        } catch (SQLException | IOException e) {
            throw new MongoServerException("failed to stream all documents with positions", e);
        }
        return allDocumentsWithPositions.stream();
    }

    @Override
    protected void handleUpdate(Long position, Document oldDocument, Document newDocument) {
        String sql = "UPDATE " + getQualifiedTablename() + " SET data = ?::json WHERE id = ?";
        try (Connection connection = backend.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, JsonConverter.toJson(newDocument));
            Object idValue = newDocument.get(idField);
            stmt.setLong(2, position);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new MongoServerException("failed to update document in " + this, e);
        }
    }

    @Override
    public void renameTo(MongoDatabase newDatabase, String newCollectionName) {
        String oldTablename = PostgresqlCollection.getTablename(getCollectionName());
        String newTablename = PostgresqlCollection.getTablename(newCollectionName);
        try (Connection connection = backend.getConnection();
             PreparedStatement stmt1 = connection.prepareStatement("ALTER TABLE " + getQualifiedTablename() + " RENAME CONSTRAINT \"pk_" + oldTablename + "\" TO \"pk_" + newTablename + "\"");
             PreparedStatement stmt2 = connection.prepareStatement("ALTER TABLE " + getQualifiedTablename() + " RENAME TO \"" + newCollectionName + "\"")
        ) {
            stmt1.executeUpdate();
            stmt2.executeUpdate();
        } catch (SQLException e) {
            throw new MongoServerException("failed to rename " + this, e);
        }

        if (!Objects.equals(getDatabaseName(), newDatabase.getDatabaseName())) {
            throw new UnsupportedOperationException();
        }

        super.renameTo(newDatabase, newCollectionName);
    }
}
