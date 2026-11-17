package de.bwaldvogel.mongo.backend;

import de.bwaldvogel.mongo.bson.Document;

public record QueryParameters(Document querySelector, int numberToSkip, int limit, int batchSize, Document projection) {

    public QueryParameters(Document querySelector, int numberToSkip, int limit) {
        this(querySelector, numberToSkip, limit, 0, null);
    }

}
