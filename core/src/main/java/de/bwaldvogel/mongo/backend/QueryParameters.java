package de.bwaldvogel.mongo.backend;

import de.bwaldvogel.mongo.bson.Document;

public class QueryParameters {
    private final Document querySelector;
    private final int numberToSkip;
    private final int limit;
    private final int batchSize;
    private final Document projection;

    QueryParameters(Document querySelector, int numberToSkip, int limit, int batchSize, Document projection) {
        this.querySelector = querySelector;
        this.numberToSkip = numberToSkip;
        this.limit = limit;
        this.batchSize = batchSize;
        this.projection = projection;
    }

    public QueryParameters(Document querySelector, int numberToSkip, int limit) {
        this(querySelector, numberToSkip, limit, 0, null);
    }

    public Document getQuerySelector() {
        return querySelector;
    }

    public int getNumberToSkip() {
        return numberToSkip;
    }

    public int getLimit() {
        return limit;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public Document getProjection() {
        return projection;
    }

}
