package de.bwaldvogel.mongo.oplog;

import de.bwaldvogel.mongo.bson.BsonTimestamp;
import de.bwaldvogel.mongo.bson.Document;

class OplogUtils {

    static BsonTimestamp getOplogTimestamp(Document document) {
        return (BsonTimestamp) document.get(OplogDocumentFields.TIMESTAMP);
    }
}
