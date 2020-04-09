package de.bwaldvogel.mongo.backend;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.InvalidOptionsException;
import de.bwaldvogel.mongo.exception.MongoServerException;

public final class CollectionOptions {

    private static final Logger log = LoggerFactory.getLogger(CollectionOptions.class);

    private static final String DEFAULT_ID_FIELD = Constants.ID_FIELD;
    private static final boolean DEFAULT_AUTO_INDEX_ID = true;
    private static final boolean DEFAULT_CAPPED = false;

    private static final long CAPPED_SIZE_GRANULARITY = 256;
    private static final long MIN_CAPPED_SIZE = CAPPED_SIZE_GRANULARITY;

    private final String idField;
    private final boolean autoIndexId;
    private final boolean capped;
    private final Long cappedSize;

    private CollectionOptions(String idField, boolean autoIndexId, boolean capped, Long cappedSize) {
        this.idField = idField;
        this.autoIndexId = autoIndexId;
        this.capped = capped;
        this.cappedSize = calculateCappedSize(cappedSize);
    }

    static Long calculateCappedSize(Long cappedSize) {
        if (cappedSize == null) {
            return null;
        }
        long value = cappedSize.longValue();
        if (value <= MIN_CAPPED_SIZE) {
            log.info("Using minimum capped size of {} bytes", MIN_CAPPED_SIZE);
            return MIN_CAPPED_SIZE;
        }
        long mod = value % CAPPED_SIZE_GRANULARITY;
        if (mod == 0) {
            return value;
        }
        long raisedCappedSize = value + (CAPPED_SIZE_GRANULARITY - mod);
        log.info("Raised capped size to {} bytes", raisedCappedSize);
        return raisedCappedSize;
    }

    public String getIdField() {
        return idField;
    }

    public static CollectionOptions withDefaults() {
        return withIdField(DEFAULT_ID_FIELD);
    }

    public static CollectionOptions withIdField(String idField) {
        return new CollectionOptions(idField, DEFAULT_AUTO_INDEX_ID, DEFAULT_CAPPED, null);
    }

    public static CollectionOptions withoutIdField() {
        return withIdField(null);
    }

    public static CollectionOptions fromQuery(Document query) {
        boolean autoIndexId = toBoolean(query.get("autoIndexId"), DEFAULT_AUTO_INDEX_ID);
        boolean capped = toBoolean(query.get("capped"), DEFAULT_CAPPED);
        Long cappedSize = toInt(query.get("size"));
        return new CollectionOptions(DEFAULT_ID_FIELD, autoIndexId, capped, cappedSize);
    }

    private static Long toInt(Object value) {
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
            throw new MongoServerException("Creating capped collections is not yet implemented");
        }

    }

}
