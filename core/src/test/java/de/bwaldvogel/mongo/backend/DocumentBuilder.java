package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.backend.QueryFilter.AND;
import static de.bwaldvogel.mongo.backend.QueryFilter.NOR;
import static de.bwaldvogel.mongo.backend.QueryFilter.OR;
import static de.bwaldvogel.mongo.backend.QueryOperator.EXISTS;
import static de.bwaldvogel.mongo.backend.QueryOperator.GREATER_THAN;
import static de.bwaldvogel.mongo.backend.QueryOperator.GREATER_THAN_OR_EQUAL;
import static de.bwaldvogel.mongo.backend.QueryOperator.IN;
import static de.bwaldvogel.mongo.backend.QueryOperator.LESS_THAN;
import static de.bwaldvogel.mongo.backend.QueryOperator.LESS_THAN_OR_EQUAL;
import static de.bwaldvogel.mongo.backend.QueryOperator.MOD;
import static de.bwaldvogel.mongo.backend.QueryOperator.NOT;
import static de.bwaldvogel.mongo.backend.QueryOperator.NOT_IN;
import static de.bwaldvogel.mongo.backend.QueryOperator.SIZE;

import java.util.List;

import de.bwaldvogel.mongo.bson.BsonRegularExpression;
import de.bwaldvogel.mongo.bson.Document;

class DocumentBuilder {

    static Document map(String key, Object value) {
        return new Document(key, value);
    }

    static Document not(Document value) {
        return map(NOT, value);
    }

    static Document regex(String pattern) {
        return new BsonRegularExpression(pattern).toDocument();
    }

    static Document regex(String pattern, String options) {
        return new BsonRegularExpression(pattern, options).toDocument();
    }

    static Document mod(int a, int b) {
        return map(MOD, list(a, b));
    }

    static Document size(int size) {
        return map(SIZE, size);
    }

    static Document lt(Number value) {
        return map(LESS_THAN, value);
    }

    static Document lte(Number value) {
        return map(LESS_THAN_OR_EQUAL, value);
    }

    static Document gte(Number value) {
        return map(GREATER_THAN_OR_EQUAL, value);
    }

    static Document gt(Number value) {
        return map(GREATER_THAN, value);
    }

    static Document or(Object... values) {
        return map(OR, values);
    }

    static Document and(Object... values) {
        return map(AND, values);
    }

    static Document nor(Object... values) {
        return map(NOR, values);
    }

    static Document allOf(Object... values) {
        return mapOfList(QueryOperator.ALL, values);
    }

    static Document elemMatch(Document document) {
        return map(QueryOperator.ELEM_MATCH.getValue(), document);
    }

    static Document in(Object... values) {
        return mapOfList(IN, values);
    }

    static Document notIn(Object... values) {
        return mapOfList(NOT_IN, values);
    }

    static Document exists() {
        return exists(true);
    }

    static Document exists(boolean value) {
        return map(EXISTS, value);
    }

    static List<Object> list(Object... values) {
        return List.of(values);
    }

    static Document map(QueryOperator operator, Object value) {
        return map(operator.getValue(), value);
    }

    static Document map(QueryFilter filter, Object... values) {
        return map(filter.getValue(), list(values));
    }

    private static Document mapOfList(QueryOperator operator, Object... values) {
        return map(operator.getValue(), list(values));
    }

}
