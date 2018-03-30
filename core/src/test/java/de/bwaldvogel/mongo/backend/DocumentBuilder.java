package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.backend.QueryFilter.AND;
import static de.bwaldvogel.mongo.backend.QueryFilter.NOR;
import static de.bwaldvogel.mongo.backend.QueryFilter.OR;
import static de.bwaldvogel.mongo.backend.QueryOperator.EQUAL;
import static de.bwaldvogel.mongo.backend.QueryOperator.EXISTS;
import static de.bwaldvogel.mongo.backend.QueryOperator.GREATER_THAN;
import static de.bwaldvogel.mongo.backend.QueryOperator.GREATER_THAN_OR_EQUAL;
import static de.bwaldvogel.mongo.backend.QueryOperator.IN;
import static de.bwaldvogel.mongo.backend.QueryOperator.LESS_THAN;
import static de.bwaldvogel.mongo.backend.QueryOperator.LESS_THAN_OR_EQUAL;
import static de.bwaldvogel.mongo.backend.QueryOperator.MOD;
import static de.bwaldvogel.mongo.backend.QueryOperator.NOT;
import static de.bwaldvogel.mongo.backend.QueryOperator.NOT_EQUALS;
import static de.bwaldvogel.mongo.backend.QueryOperator.NOT_IN;
import static de.bwaldvogel.mongo.backend.QueryOperator.SIZE;

import java.util.Arrays;
import java.util.List;

import de.bwaldvogel.mongo.bson.BsonRegularExpression;
import de.bwaldvogel.mongo.bson.Document;

public class DocumentBuilder {

    public static Document map(String key, Object value) {
        return new Document(key, value);
    }

    public static Document map() {
        return new Document();
    }

    public static Document not(Document value) {
        return map(NOT, value);
    }

    public static Document regex(String pattern) {
        return new BsonRegularExpression(pattern).toDocument();
    }

    public static Document regex(String pattern, String options) {
        return new BsonRegularExpression(pattern, options).toDocument();
    }

    public static Document mod(int a, int b) {
        return map(MOD, list(a, b));
    }

    public static Document size(int size) {
        return map(SIZE, size);
    }

    public static Document eq(Object value) {
        return map(EQUAL, value);
    }

    public static Document ne(Object value) {
        return map(NOT_EQUALS, value);
    }

    public static Document lt(Number value) {
        return map(LESS_THAN, value);
    }

    public static Document lte(Number value) {
        return map(LESS_THAN_OR_EQUAL, value);
    }

    public static Document gte(Number value) {
        return map(GREATER_THAN_OR_EQUAL, value);
    }

    public static Document gt(Number value) {
        return map(GREATER_THAN, value);
    }

    public static Document or(Object... values) {
        return map(OR, values);
    }

    public static Document and(Object... values) {
        return map(AND, values);
    }

    public static Document nor(Object... values) {
        return map(NOR, values);
    }

    public static Document allOf(Object... values) {
        return mapOfList(QueryOperator.ALL, values);
    }

    public static Document elemMatch(Document document) {
        return map(QueryOperator.ELEM_MATCH.getValue(), document);
    }

    public static Document in(Object... values) {
        return mapOfList(IN, values);
    }

    public static Document notIn(Object... values) {
        return mapOfList(NOT_IN, values);
    }

    public static Document exists() {
        return exists(true);
    }

    public static Document exists(boolean value) {
        return map(EXISTS, value);
    }

    public static List<Object> list(Object... values) {
        return Arrays.asList(values);
    }

    public static Document map(QueryOperator operator, Object value) {
        return map(operator.getValue(), value);
    }

    public static Document map(QueryFilter filter, Object... values) {
        return map(filter.getValue(), list(values));
    }

    private static Document mapOfList(QueryOperator operator, Object... values) {
        return map(operator.getValue(), list(values));
    }

}
