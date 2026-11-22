package de.bwaldvogel.mongo.backend;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.InvalidOptionsException;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerException;

public record CollectionOptions(String idField, boolean autoIndexId, boolean capped,
                                Long cappedSize, Long cappedMaxDocuments) {

    private static final String DEFAULT_ID_FIELD = Constants.ID_FIELD;
    private static final boolean DEFAULT_AUTO_INDEX_ID = true;
    private static final boolean DEFAULT_CAPPED = false;

    public Long getCappedMaxDocuments() {
        return cappedMaxDocuments;
    }

    public Document toDocument() {
        Document document = new Document();
        if (capped) {
            document.put("capped", true);
            document.put("size", cappedSize);
            if (cappedMaxDocuments != null && cappedMaxDocuments > 0) {
                document.put("max", cappedMaxDocuments);
            }
        }
        return document;
    }

    public boolean isCapped() {
        return capped;
    }

    public Long getCappedSize() {
        return cappedSize;
    }

    public String getIdField() {
        return idField;
    }

    public static CollectionOptions withDefaults() {
        return withIdField(DEFAULT_ID_FIELD);
    }

    public static CollectionOptions withIdField(String idField) {
        return new CollectionOptions(idField, DEFAULT_AUTO_INDEX_ID, DEFAULT_CAPPED, null, null);
    }

    public static CollectionOptions withoutIdField() {
        return withIdField(null);
    }

    public static CollectionOptions fromQuery(Document query) {
        boolean autoIndexId = toBoolean(query.get("autoIndexId"), DEFAULT_AUTO_INDEX_ID);
        boolean capped = toBoolean(query.get("capped"), DEFAULT_CAPPED);
        Long cappedSize = toLong(query.get("size"));
        return new CollectionOptions(DEFAULT_ID_FIELD, autoIndexId, capped, cappedSize, toLong(query.get("max")));
    }

    private static Long toLong(Object value) {
        if (value == null) {
            return null;
        }
        return ((Number) value).longValue();
    }

    private static boolean toBoolean(Object value, boolean defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        return Utils.isTrue(value);
    }

    public void validate() {
        if (!autoIndexId) {
            throw new MongoServerException("Disabling autoIndexId is not yet implemented");
        }
        if (capped) {
            if (cappedSize == null) {
                throw new InvalidOptionsException("the 'size' field is required when 'capped' is true");
            }
            if (cappedSize <= 0) {
                throw new MongoServerError(51024, "BSON field 'create.size' value must be > 0, actual value '" + cappedSize + "'");
            }
        }
    }

}
