package de.bwaldvogel.mongo.backend.aggregation;

import de.bwaldvogel.mongo.bson.Document;

interface Accumulator {

    void initialize(Document result);

    void aggregate(Document result, Document document);

}
