package de.bwaldvogel.mongo.backend.aggregation;

import de.bwaldvogel.mongo.bson.Document;

class SimpleSumAccumulation implements Accumulator {

    private final String key;

    SimpleSumAccumulation(String key) {
        this.key = key;
    }

    @Override
    public void initialize(Document result) {
        result.put(key, 0L);
    }

    @Override
    public void aggregate(Document result, Document document) {
        Long count = (Long) result.get(key);
        result.put(key, count + 1);
    }
}
