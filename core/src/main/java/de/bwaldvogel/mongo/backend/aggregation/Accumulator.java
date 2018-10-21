package de.bwaldvogel.mongo.backend.aggregation;

import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.bson.Document;

abstract class Accumulator {

    private final Object expression;

    Accumulator(Object expression) {
        this.expression = expression;
    }

    void aggregate(Document document) {
        if (expression instanceof String && ((String) expression).startsWith("$")) {
            String value = ((String) expression).substring(1);
            aggregate(Utils.getSubdocumentValue(document, value));
        } else {
            aggregate(expression);
        }
    }

    abstract void aggregate(Object value);

    abstract Object getResult();
}
