package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.backend.QueryFilter.AND;
import static de.bwaldvogel.mongo.backend.QueryFilter.NOR;
import static de.bwaldvogel.mongo.backend.QueryFilter.OR;
import static de.bwaldvogel.mongo.backend.QueryOperator.GREATER_THAN;
import static de.bwaldvogel.mongo.backend.QueryOperator.GREATER_THAN_OR_EQUAL;
import static de.bwaldvogel.mongo.backend.QueryOperator.IN;
import static de.bwaldvogel.mongo.backend.QueryOperator.LESS_THAN;
import static de.bwaldvogel.mongo.backend.QueryOperator.LESS_THAN_OR_EQUAL;
import static de.bwaldvogel.mongo.backend.QueryOperator.NOT_IN;

import java.util.Arrays;
import java.util.List;

import de.bwaldvogel.mongo.bson.Document;

public class DocumentBuilder {

    public static Document map(String key, Object value) {
        return new Document(key, value);
    }

    public static Document map() {
        return new Document();
    }

    public static Document regex(String pattern) {
        return map(QueryOperator.REGEX, pattern);
    }

    public static Document regex(String pattern, String options) {
        return regex(pattern).append("$options", options);
    }

    public static Document lt(Object value) {
        return map(LESS_THAN, value);
    }

    public static Document lte(Object value) {
        return map(LESS_THAN_OR_EQUAL, value);
    }

    public static Document gte(Object value) {
        return map(GREATER_THAN_OR_EQUAL, value);
    }

    public static Document gt(Object value) {
        return map(GREATER_THAN, value);
    }

    public static Document or(Object... values) {
        return mapOfList(OR, values);
    }

    public static Document and(Object... values) {
        return mapOfList(AND, values);
    }

    public static Document nor(Object... values) {
        return mapOfList(NOR, values);
    }

    public static Document allOf(Object... values) {
        return mapOfList(QueryOperator.ALL, values);
    }

    public static Document in(Object... values) {
        return mapOfList(IN, values);
    }

    public static Document notIn(Object... values) {
        return mapOfList(NOT_IN, values);
    }

    public static List<Object> list(Object... values) {
        return Arrays.asList(values);
    }

    private static Document map(QueryOperator operator, Object value) {
        return map(operator.getValue(), value);
    }

    private static Document mapOfList(QueryFilter filter, Object... values) {
        return map(filter.getValue(), list(values));
    }

    private static Document mapOfList(String key, Object... values) {
        return map(key, list(values));
    }

    private static Document mapOfList(QueryOperator operator, Object... values) {
        return map(operator.getValue(), list(values));
    }


}
