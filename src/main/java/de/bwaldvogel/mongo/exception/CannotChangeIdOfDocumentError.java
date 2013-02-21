package de.bwaldvogel.mongo.exception;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import de.bwaldvogel.mongo.backend.Constants;

public class CannotChangeIdOfDocumentError extends MongoServerError {

    private static final long serialVersionUID = 2696896807939190889L;

    public CannotChangeIdOfDocumentError(BSONObject document, BSONObject newDocument) {
        super(13596, "cannot change _id of a document old:" + getId(document) + " new:" + getId(newDocument));
    }

    private static BSONObject getId(BSONObject document) {
        return new BasicBSONObject(Constants.ID_FIELD, document.get(Constants.ID_FIELD));
    }

}
