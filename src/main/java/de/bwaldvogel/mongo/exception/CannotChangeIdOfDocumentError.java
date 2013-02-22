package de.bwaldvogel.mongo.exception;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;

public class CannotChangeIdOfDocumentError extends MongoServerError {

    private static final long serialVersionUID = 2696896807939190889L;

    public CannotChangeIdOfDocumentError(BSONObject document, BSONObject newDocument, String idField) {
        super(13596, "cannot change _id of a document old:" + getId(document, idField) + " new:" + getId(newDocument, idField));
    }

    private static BSONObject getId(BSONObject document, String idField) {
        return new BasicBSONObject(idField, document.get(idField));
    }

}
