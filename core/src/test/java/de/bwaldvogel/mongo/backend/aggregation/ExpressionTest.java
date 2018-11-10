package de.bwaldvogel.mongo.backend.aggregation;

import static de.bwaldvogel.mongo.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

import org.assertj.core.data.Offset;
import org.junit.Test;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.Missing;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class ExpressionTest {

    @Test
    public void testEvaluateSimpleValue() throws Exception {
        assertThat(Expression.evaluate(1, json(""))).isEqualTo(1);
        assertThat(Expression.evaluate(null, json(""))).isNull();
        assertThat(Expression.evaluate("abc", json(""))).isEqualTo("abc");
        assertThat(Expression.evaluate("$a", json("a: 123"))).isEqualTo(123);
        assertThat(Expression.evaluate("$a", json(""))).isInstanceOf(Missing.class);
        assertThat(Expression.evaluate("$a", json("a: null"))).isNull();
        assertThat(Expression.evaluate(json("a: 1, b: 2"), json("a: -2"))).isEqualTo(json("a: 1, b: 2"));
    }

    @Test
    public void testEvaluateAbs() throws Exception {
        assertThat(Expression.evaluate(json("$abs: '$a'"), json("a: -2"))).isEqualTo(2);
        assertThat(Expression.evaluate(json("$abs: '$a'"), json("a: -2.5"))).isEqualTo(2.5);
        assertThat(Expression.evaluate(new Document("$abs", 123L), json(""))).isEqualTo(123L);
        assertThat(Expression.evaluate(json("$abs: null"), json(""))).isNull();
        assertThat(Expression.evaluate(json("abs: {$abs: '$a'}"), json("a: -25"))).isEqualTo(json("abs: 25"));

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$abs: '$a', $ceil: '$b'"), json("")))
            .withMessage("[Error 15983] An object representing an expression must have exactly one field: {\"$abs\" : \"$a\", \"$ceil\" : \"$b\"}");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$abs: 'abc'"), json("")))
            .withMessage("[Error 28765] $abs only supports numeric types, not class java.lang.String");
    }

    @Test
    public void testEvaluateAdd() throws Exception {
        assertThat(Expression.evaluate(json("$add: ['$a', '$b']"), json("a: 7, b: 5"))).isEqualTo(12);
        assertThat(Expression.evaluate(json("$add: [7.5, 3]"), json(""))).isEqualTo(10.5);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$add: []"), json("")))
            .withMessage("[Error 16020] Expression $add takes exactly 2 arguments. 0 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$add: [1]"), json("")))
            .withMessage("[Error 16020] Expression $add takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$add: 123"), json("")))
            .withMessage("[Error 16020] Expression $add takes exactly 2 arguments. 1 were passed in.");
    }

    @Test
    public void testEvaluateAnd() throws Exception {
        assertThat(Expression.evaluate(json("$and: [ 1, 'green' ]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$and: [ ]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$and: [ [ null ], [ false ], [ 0 ] ]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$and: 'abc'"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$and: true"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$and: [ { $gt: [ '$qty', 100 ] }, { $lt: [ '$qty', 250 ] } ]"), json("qty: 150"))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$and: [ { $gt: [ '$qty', 100 ] }, { $lt: [ '$qty', 250 ] } ]"), json("qty: 300"))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$and: false"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$and: null"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$and: [ null, true ]"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$and: [ 0, true ]"), json(""))).isEqualTo(false);
    }

    @Test
    public void testEvaluateAnyElementTrue() throws Exception {
        assertThat(Expression.evaluate(json("$anyElementTrue: [ [ true, false] ]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$anyElementTrue: [ '$items' ]"), json("items: [false, true]"))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$anyElementTrue: [ [ [ false ] ] ]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$anyElementTrue: [ '$items' ]"), json("items: [false, false]"))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$anyElementTrue: [ [ null, false, 0 ] ]"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$anyElementTrue: [ [ ] ]"), json(""))).isEqualTo(false);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$anyElementTrue: null"), json("")))
            .withMessage("[Error 17041] $anyElementTrue's argument must be an array, but is null");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$anyElementTrue: [null]"), json("")))
            .withMessage("[Error 17041] $anyElementTrue's argument must be an array, but is null");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$anyElementTrue: 'abc'"), json("")))
            .withMessage("[Error 17041] $anyElementTrue's argument must be an array, but is java.lang.String");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$anyElementTrue: [1, 2]"), json("")))
            .withMessage("[Error 16020] Expression $anyElementTrue takes exactly 1 arguments. 2 were passed in.");
    }

    @Test
    public void testEvaluateAllElementsTrue() throws Exception {
        assertThat(Expression.evaluate(json("$allElementsTrue: [ [ true, 1, 'someString' ] ]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$allElementsTrue: [ [ [ false ] ] ]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$allElementsTrue: [ [ ] ]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$allElementsTrue: [ '$items' ]"), json("items: [true]"))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$allElementsTrue: [ [ null, false, 0 ] ]"), json(""))).isEqualTo(false);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$allElementsTrue: null"), json("")))
            .withMessage("[Error 17040] $allElementsTrue's argument must be an array, but is null");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$allElementsTrue: [null]"), json("")))
            .withMessage("[Error 17040] $allElementsTrue's argument must be an array, but is null");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$allElementsTrue: 'abc'"), json("")))
            .withMessage("[Error 17040] $allElementsTrue's argument must be an array, but is java.lang.String");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$allElementsTrue: [1, 2]"), json("")))
            .withMessage("[Error 16020] Expression $allElementsTrue takes exactly 1 arguments. 2 were passed in.");
    }

    @Test
    public void testEvaluateArrayElemAt() throws Exception {
        assertThat(Expression.evaluate(json("$arrayElemAt: [ [ 1, 2, 3 ], 0 ]"), json(""))).isEqualTo(1);
        assertThat(Expression.evaluate(json("$arrayElemAt: [ [ 1, 2, 3 ], 1.0 ]"), json(""))).isEqualTo(2);
        assertThat(Expression.evaluate(json("$arrayElemAt: [ [ 1, 2, 3 ], -2 ]"), json(""))).isEqualTo(2);
        assertThat(Expression.evaluate(json("$arrayElemAt: [ [ 1, 2, 3 ], 15 ]"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$arrayElemAt: [ '$items', 1 ]"), json("items: ['a', 'b', 'c']"))).isEqualTo("b");
        assertThat(Expression.evaluate(json("$arrayElemAt: [ '$items', '$pos' ]"), json("items: ['a', 'b', 'c'], pos: -1"))).isEqualTo("c");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$arrayElemAt: null"), json("")))
            .withMessage("[Error 16020] Expression $arrayElemAt takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$arrayElemAt: [1, 2, 3]"), json("")))
            .withMessage("[Error 16020] Expression $arrayElemAt takes exactly 2 arguments. 3 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$arrayElemAt: ['a', 'b']"), json("")))
            .withMessage("[Error 28689] $arrayElemAt's first argument must be an array, but is java.lang.String");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$arrayElemAt: [['a', 'b'], 'b']"), json("")))
            .withMessage("[Error 28690] $arrayElemAt's second argument must be a numeric value, but is java.lang.String");
    }

    @Test
    public void testEvaluateCeil() throws Exception {
        assertThat(Expression.evaluate(json("$ceil: '$a'"), json("a: 2.5"))).isEqualTo(3);
        assertThat(Expression.evaluate(json("$ceil: 42"), json(""))).isEqualTo(42);
        assertThat(Expression.evaluate(json("$ceil: 42.3"), json(""))).isEqualTo(43);
        assertThat(Expression.evaluate(new Document("$ceil", (double) Long.MAX_VALUE), json(""))).isEqualTo(Long.MAX_VALUE);
        assertThat(Expression.evaluate(new Document("$ceil", (double) Long.MIN_VALUE), json(""))).isEqualTo(Long.MIN_VALUE);
        assertThat(Expression.evaluate(json("$ceil: null"), json(""))).isNull();
        assertThat(Expression.evaluate(json("ceil: {$ceil: '$a'}"), json("a: -25.5"))).isEqualTo(json("ceil: -25"));

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$ceil: 'abc'"), json("")))
            .withMessage("[Error 28765] $ceil only supports numeric types, not class java.lang.String");
    }

    @Test
    public void testEvaluateCmp() throws Exception {
        assertThat(Expression.evaluate(json("$cmp: [20, 10]"), json(""))).isEqualTo(1);
        assertThat(Expression.evaluate(json("$cmp: [20, 20]"), json(""))).isEqualTo(0);
        assertThat(Expression.evaluate(json("$cmp: [10, 20]"), json(""))).isEqualTo(-1);
        assertThat(Expression.evaluate(json("$cmp: [ '$a', '$b' ]"), json("a: 10, b: 5"))).isEqualTo(1);
        assertThat(Expression.evaluate(json("$cmp: ['b', 'a']"), json(""))).isEqualTo(1);
        assertThat(Expression.evaluate(json("$cmp: ['a', 'b']"), json(""))).isEqualTo(-1);
        assertThat(Expression.evaluate(json("$cmp: [ '$qty', 250 ]"), json("qty: 500"))).isEqualTo(1);
        assertThat(Expression.evaluate(json("$cmp: [ '$qty', 250 ]"), json("qty: 100"))).isEqualTo(-1);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$cmp: 'abc'"), json("")))
            .withMessage("[Error 16020] Expression $cmp takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$cmp: ['a', 'b', 'c']"), json("")))
            .withMessage("[Error 16020] Expression $cmp takes exactly 2 arguments. 3 were passed in.");
    }

    @Test
    public void testEvaluateConcat() throws Exception {
        assertThat(Expression.evaluate(json("$concat: null"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$concat: ['A', 'B', 'C']"), json(""))).isEqualTo("ABC");
        assertThat(Expression.evaluate(json("$concat: ['$a', '-', '$b']"), json("a: 'A', b: 'B'"))).isEqualTo("A-B");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$concat: 1"), json("")))
            .withMessage("[Error 16702] $concat only supports strings, not java.lang.Integer");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$concat: [1]"), json("")))
            .withMessage("[Error 16702] $concat only supports strings, not java.lang.Integer");
    }

    @Test
    public void testEvaluateConcatArrays() throws Exception {
        assertThat(Expression.evaluate(json("$concatArrays: null"), json(""))).isNull();

        assertThat(Expression.evaluate(json("$concatArrays: [ [ 'hello', ' '], [ 'world' ] ]"), json("")))
            .isEqualTo(Arrays.asList("hello", " ", "world"));

        assertThat(Expression.evaluate(json("$concatArrays: [ [ 'hello', ' '], [ [ 'world' ], 'again'] ]"), json("")))
            .isEqualTo(Arrays.asList("hello", " ", Collections.singletonList("world"), "again"));

        assertThat(Expression.evaluate(json("$concatArrays: [ '$a', '$b' ]"), json("a: [1, 2], b: [3, 4]")))
            .isEqualTo(Arrays.asList(1, 2, 3, 4));

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$concatArrays: 1"), json("")))
            .withMessage("[Error 28664] $concatArrays only supports arrays, not java.lang.Integer");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$concatArrays: [1]"), json("")))
            .withMessage("[Error 28664] $concatArrays only supports arrays, not java.lang.Integer");
    }

    @Test
    public void testEvaluateCond() throws Exception {
        assertThat(Expression.evaluate(json("$cond: { if: { $gte: [ '$qty', 250 ] }, then: 30, else: 20}"), json("qty: 100")))
            .isEqualTo(20);

        assertThat(Expression.evaluate(json("$cond: { if: { $gte: [ '$qty', 250 ] }, then: 30, else: 20}"), json("qty: 300")))
            .isEqualTo(30);

        assertThat(Expression.evaluate(json("$cond: { if: { $gte: [ '$qty', 250 ] }, then: '$qty', else: 20}"), json("qty: 300")))
            .isEqualTo(300);

        assertThat(Expression.evaluate(json("$cond: { if: { $gte: [ '$qty', 250 ] }, then: 10, else: '$qty'}"), json("qty: 200")))
            .isEqualTo(200);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$cond: null"), json("")))
            .withMessage("[Error 16020] Expression $cond takes exactly 3 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$cond: {}"), json("")))
            .withMessage("[Error 17080] Missing 'if' parameter to $cond");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$cond: {then: 1, else: 1}"), json("")))
            .withMessage("[Error 17080] Missing 'if' parameter to $cond");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$cond: {if: 1, else: 1}"), json("")))
            .withMessage("[Error 17080] Missing 'then' parameter to $cond");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$cond: {if: 1, then: 1}"), json("")))
            .withMessage("[Error 17080] Missing 'else' parameter to $cond");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$cond: {if: 1, then: 1, else: 1, foo: 1}"), json("")))
            .withMessage("[Error 17083] Unrecognized parameter to $cond: foo");
    }

    @Test
    public void testEvaluateEq() throws Exception {
        assertThat(Expression.evaluate(json("$eq: [20, 20]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$eq: [20, 10]"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$eq: [null, null]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$eq: [ '$a', '$b' ]"), json("a: 10, b: 10"))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$eq: [ '$qty', 250 ]"), json("qty: 250"))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$eq: [ '$qty', 250 ]"), json("qty: 100"))).isEqualTo(false);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$eq: 'abc'"), json("")))
            .withMessage("[Error 16020] Expression $eq takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$eq: ['a', 'b', 'c']"), json("")))
            .withMessage("[Error 16020] Expression $eq takes exactly 2 arguments. 3 were passed in.");
    }

    @Test
    public void testEvaluateMinute() throws Exception {
        assertThat(Expression.evaluate(json("$minute: '$a'"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$minute: '$a'"), new Document("a", toDate("2018-07-03T14:10:00Z")))).isEqualTo(10);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$minute: '$a'"), json("a: 'abc'")))
            .withMessage("[Error 16006] can't convert from java.lang.String to Date");
    }

    @Test
    public void testEvaluateMonth() throws Exception {
        assertThat(Expression.evaluate(json("$month: '$a'"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$month: '$a'"), new Document("a", toDate("2018-07-03T14:00:00Z")))).isEqualTo(7);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$month: '$a'"), json("a: 'abc'")))
            .withMessage("[Error 16006] can't convert from java.lang.String to Date");
    }

    @Test
    public void testEvaluateNe() throws Exception {
        assertThat(Expression.evaluate(json("$ne: [20, 20]"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$ne: [20, 10]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$ne: [20, 'a']"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$ne: [ '$a', '$b' ]"), json("a: 10, b: 10"))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$ne: [ '$qty', 250 ]"), json("qty: 250"))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$ne: [ '$qty', 250 ]"), json("qty: 100"))).isEqualTo(true);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$ne: 'abc'"), json("")))
            .withMessage("[Error 16020] Expression $ne takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$ne: ['a', 'b', 'c']"), json("")))
            .withMessage("[Error 16020] Expression $ne takes exactly 2 arguments. 3 were passed in.");
    }

    @Test
    public void testEvaluateGt() throws Exception {
        assertThat(Expression.evaluate(json("$gt: [20, 10]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$gt: [20, 20]"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$gt: [ '$a', '$b' ]"), json("a: 10, b: 5"))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$gt: ['b', 'a']"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$gt: ['a', 'b']"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$gt: [ '$qty', 250 ]"), json("qty: 500"))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$gt: [ '$qty', 250 ]"), json("qty: 100"))).isEqualTo(false);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$gt: 'abc'"), json("")))
            .withMessage("[Error 16020] Expression $gt takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$gt: ['a', 'b', 'c']"), json("")))
            .withMessage("[Error 16020] Expression $gt takes exactly 2 arguments. 3 were passed in.");
    }

    @Test
    public void testEvaluateGte() throws Exception {
        assertThat(Expression.evaluate(json("$gte: [20, 10]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$gte: [20, 20]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$gte: [20, 21]"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$gte: [ '$qty', 250 ]"), json("qty: 500"))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$gte: [ '$qty', 250 ]"), json("qty: 100"))).isEqualTo(false);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$gte: 'abc'"), json("")))
            .withMessage("[Error 16020] Expression $gte takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$gte: ['a', 'b', 'c']"), json("")))
            .withMessage("[Error 16020] Expression $gte takes exactly 2 arguments. 3 were passed in.");
    }

    @Test
    public void testEvaluateHour() throws Exception {
        assertThat(Expression.evaluate(json("$hour: '$a'"), json(""))).isNull();
        int expectedHour = ZonedDateTime.ofInstant(Instant.parse("2018-07-03T14:10:00Z"), ZoneId.systemDefault()).toLocalTime().getHour();
        assertThat(Expression.evaluate(json("$hour: '$a'"), new Document("a", toDate("2018-07-03T14:10:00Z")))).isEqualTo(expectedHour);
        assertThat(Expression.evaluate(json("$hour: {date: '$a'}"), new Document("a", toDate("2018-07-03T14:10:00Z")))).isEqualTo(expectedHour);
        assertThat(Expression.evaluate(json("$hour: {date: '$a', timezone: 'UTC'}"), new Document("a", toDate("2018-07-03T14:10:00Z")))).isEqualTo(14);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$hour: '$a'"), json("a: 'abc'")))
            .withMessage("[Error 16006] can't convert from java.lang.String to Date");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$hour: {}"), json("")))
            .withMessage("[Error 40539] missing 'date' argument to $hour, provided: {}");
    }

    @Test
    public void testEvaluateLt() throws Exception {
        assertThat(Expression.evaluate(json("$lt: [10, 20]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$lt: [20, 20]"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$lt: ['$qty', 250]"), json("qty: 100"))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$lt: ['$qty', 250]"), json("qty: 500"))).isEqualTo(false);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$lt: 'abc'"), json("")))
            .withMessage("[Error 16020] Expression $lt takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$lt: ['a', 'b', 'c']"), json("")))
            .withMessage("[Error 16020] Expression $lt takes exactly 2 arguments. 3 were passed in.");
    }

    @Test
    public void testEvaluateLte() throws Exception {
        assertThat(Expression.evaluate(json("$lte: [10, 20]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$lte: [20, 20]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$lte: [21, 20]"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$lte: [ '$qty', 250 ]"), json("qty: 100"))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$lte: [ '$qty', 250 ]"), json("qty: 500"))).isEqualTo(false);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$lte: 'abc'"), json("")))
            .withMessage("[Error 16020] Expression $lte takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$lte: ['a', 'b', 'c']"), json("")))
            .withMessage("[Error 16020] Expression $lte takes exactly 2 arguments. 3 were passed in.");
    }

    @Test
    public void testEvaluateSecond() throws Exception {
        assertThat(Expression.evaluate(json("$second: '$a'"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$second: '$a'"), new Document("a", toDate("2018-07-03T14:10:23Z")))).isEqualTo(23);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$second: '$a'"), json("a: 'abc'")))
            .withMessage("[Error 16006] can't convert from java.lang.String to Date");
    }

    @Test
    public void testEvaluateSum() throws Exception {
        assertThat(Expression.evaluate(json("$sum: null"), json(""))).isEqualTo(0);
        assertThat(Expression.evaluate(json("$sum: ''"), json(""))).isEqualTo(0);
        assertThat(Expression.evaluate(json("$sum: 5"), json(""))).isEqualTo(5);
        assertThat(Expression.evaluate(json("$sum: [1, 'foo', 2]"), json(""))).isEqualTo(3);
        assertThat(Expression.evaluate(json("$sum: ['$a', '$b']"), json("a: 7, b: 5"))).isEqualTo(12);
        assertThat(Expression.evaluate(json("$sum: []"), json(""))).isEqualTo(0);
    }

    @Test
    public void testEvaluateSize() throws Exception {
        assertThat(Expression.evaluate(json("$size: ['$a', '$b']"), json("a: 7, b: 5"))).isEqualTo(2);
        assertThat(Expression.evaluate(json("$size: [7.5, 3]"), json(""))).isEqualTo(2);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$size: null"), json("")))
            .withMessage("[Error 17124] The argument to $size must be an array, but was of type: null");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$size: 'abc'"), json("")))
            .withMessage("[Error 17124] The argument to $size must be an array, but was of type: java.lang.String");
    }

    @Test
    public void testEvaluateSubtract() throws Exception {
        assertThat(Expression.evaluate(json("$subtract: ['$a', '$b']"), json("a: 7, b: 5"))).isEqualTo(2);
        assertThat(Expression.evaluate(json("$subtract: [7.5, 3]"), json(""))).isEqualTo(4.5);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$subtract: []"), json("")))
            .withMessage("[Error 16020] Expression $subtract takes exactly 2 arguments. 0 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$subtract: [1]"), json("")))
            .withMessage("[Error 16020] Expression $subtract takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$subtract: 123"), json("")))
            .withMessage("[Error 16020] Expression $subtract takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$subtract: ['a', 'b']"), json("")))
            .withMessage("[Error 16556] cant $subtract a java.lang.String from a java.lang.String");
    }

    @Test
    public void testEvaluateSqrt() throws Exception {
        assertThat((double) Expression.evaluate(json("$sqrt: '$a'"), json("a: 2.5"))).isEqualTo(1.581, Offset.offset(0.001));
        assertThat(Expression.evaluate(json("$sqrt: 16"), json(""))).isEqualTo(4.0);
        assertThat(Expression.evaluate(json("$sqrt: null"), json(""))).isNull();
        assertThat(Expression.evaluate(json("sqrt: {$sqrt: '$a'}"), json("a: 25"))).isEqualTo(json("sqrt: 5.0"));

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$sqrt: 'abc'"), json("")))
            .withMessage("[Error 28765] $sqrt only supports numeric types, not class java.lang.String");
    }

    @Test
    public void testEvaluateYear() throws Exception {
        assertThat(Expression.evaluate(json("$year: '$a'"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$year: '$a'"), new Document("a", toDate("2018-07-03T14:00:00Z")))).isEqualTo(2018);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$year: '$a'"), json("a: 'abc'")))
            .withMessage("[Error 16006] can't convert from java.lang.String to Date");
    }

    @Test
    public void testEvaluateDayOfWeek() throws Exception {
        assertThat(Expression.evaluate(json("$dayOfWeek: '$a'"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$dayOfWeek: '$a'"), new Document("a", toDate("2018-01-01T14:00:00Z")))).isEqualTo(1);
        assertThat(Expression.evaluate(json("$dayOfWeek: '$a'"), new Document("a", toDate("2014-02-03T14:00:00Z")))).isEqualTo(1);
        assertThat(Expression.evaluate(json("$dayOfWeek: '$a'"), new Document("a", toDate("2018-11-08T22:00:00Z")))).isEqualTo(4);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$dayOfWeek: '$a'"), json("a: 'abc'")))
            .withMessage("[Error 16006] can't convert from java.lang.String to Date");
    }

    @Test
    public void testEvaluateDayOfMonth() throws Exception {
        assertThat(Expression.evaluate(json("$dayOfMonth: '$a'"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$dayOfMonth: '$a'"), new Document("a", toDate("2018-01-01T14:00:00Z")))).isEqualTo(1);
        assertThat(Expression.evaluate(json("$dayOfMonth: '$a'"), new Document("a", toDate("2014-02-03T14:00:00Z")))).isEqualTo(3);
        assertThat(Expression.evaluate(json("$dayOfMonth: '$a'"), new Document("a", toDate("2018-11-08T22:00:00Z")))).isEqualTo(8);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$dayOfMonth: '$a'"), json("a: 'abc'")))
            .withMessage("[Error 16006] can't convert from java.lang.String to Date");
    }

    @Test
    public void testEvaluateDayOfYear() throws Exception {
        assertThat(Expression.evaluate(json("$dayOfYear: '$a'"), json(""))).isNull();
        assertThat(Expression.evaluate(json("$dayOfYear: '$a'"), new Document("a", toDate("2018-01-01T14:00:00Z")))).isEqualTo(1);
        assertThat(Expression.evaluate(json("$dayOfYear: '$a'"), new Document("a", toDate("2014-02-03T14:00:00Z")))).isEqualTo(34);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$dayOfYear: '$a'"), json("a: 'abc'")))
            .withMessage("[Error 16006] can't convert from java.lang.String to Date");
    }

    @Test
    public void testEvaluateLiteral() throws Exception {
        assertThat(Expression.evaluate(json("$literal: { $add: [ 2, 3 ] }"), json(""))).isEqualTo(json("'$add' : [ 2, 3 ]"));
        assertThat(Expression.evaluate(json("$literal:  { $literal: 1 }"), json(""))).isEqualTo(json("'$literal' : 1"));
    }

    @Test
    public void testEvaluateIllegalExpression() throws Exception {
        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$foo: '$a'"), json("")))
            .withMessage("[Error 168] Unrecognized expression '$foo'");
    }

    private static Date toDate(String instant) {
        return Date.from(Instant.parse(instant));
    }

}