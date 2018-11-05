package de.bwaldvogel.mongo.backend.aggregation;

import static de.bwaldvogel.mongo.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.time.Instant;
import java.util.Date;

import org.junit.Test;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class ExpressionTest {

    @Test
    public void testEvaluateSimpleValue() throws Exception {
        assertThat(Expression.evaluate(1, json("{}"))).isEqualTo(1);
        assertThat(Expression.evaluate(null, json("{}"))).isNull();
        assertThat(Expression.evaluate("abc", json("{}"))).isEqualTo("abc");
        assertThat(Expression.evaluate("$a", json("a: 123"))).isEqualTo(123);
        assertThat(Expression.evaluate("$a", json("{}"))).isNull();
        assertThat(Expression.evaluate(json("a: 1, b: 2"), json("a: -2"))).isEqualTo(json("a: 1, b: 2"));
    }

    @Test
    public void testEvaluateAbs() throws Exception {
        assertThat(Expression.evaluate(json("$abs: '$a'"), json("a: -2"))).isEqualTo(2);
        assertThat(Expression.evaluate(json("$abs: '$a'"), json("a: -2.5"))).isEqualTo(2.5);
        assertThat(Expression.evaluate(new Document("$abs", 123L), json("{}"))).isEqualTo(123L);
        assertThat(Expression.evaluate(json("$abs: null"), json("{}"))).isNull();
        assertThat(Expression.evaluate(json("abs: {$abs: '$a'}"), json("a: -25"))).isEqualTo(json("abs: 25"));

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$abs: '$a', $ceil: '$b'"), json("{}")))
            .withMessage("An object representing an expression must have exactly one field: {\"$abs\" : \"$a\", \"$ceil\" : \"$b\"}");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$abs: 'abc'"), json("{}")))
            .withMessage("$abs only supports numeric types, not class java.lang.String");
    }

    @Test
    public void testEvaluateAdd() throws Exception {
        assertThat(Expression.evaluate(json("$add: ['$a', '$b']"), json("a: 7, b: 5"))).isEqualTo(12);
        assertThat(Expression.evaluate(json("$add: [7.5, 3]"), json("{}"))).isEqualTo(10.5);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$add: []"), json("{}")))
            .withMessage("Expression $add takes exactly 2 arguments. 0 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$add: [1]"), json("{}")))
            .withMessage("Expression $add takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$add: 123"), json("{}")))
            .withMessage("Expression $add takes exactly 2 arguments. 1 were passed in.");
    }

    @Test
    public void testEvaluateAnd() throws Exception {
        assertThat(Expression.evaluate(json("$and: [ 1, 'green' ]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$and: [ ]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$and: [ [ null ], [ false ], [ 0 ] ]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$and: 'abc'"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$and: true"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$and: false"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$and: null"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$and: [ null, true ]"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$and: [ 0, true ]"), json(""))).isEqualTo(false);
    }

    @Test
    public void testEvaluateAnyElementTrue() throws Exception {
        assertThat(Expression.evaluate(json("$anyElementTrue: [ [ true, false] ]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$anyElementTrue: [ [ [ false ] ] ]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$anyElementTrue: [ [ null, false, 0 ] ]"), json(""))).isEqualTo(false);
        assertThat(Expression.evaluate(json("$anyElementTrue: [ [ ] ]"), json(""))).isEqualTo(false);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$anyElementTrue: null"), json("a: 'abc'")))
            .withMessage("$anyElementTrue's argument must be an array, but is null");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$anyElementTrue: [null]"), json("a: 'abc'")))
            .withMessage("$anyElementTrue's argument must be an array, but is null");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$anyElementTrue: '$a'"), json("a: 'abc'")))
            .withMessage("$anyElementTrue's argument must be an array, but is java.lang.String");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$anyElementTrue: [1, 2]"), json("a: 'abc'")))
            .withMessage("Expression $anyElementTrue takes exactly 1 arguments. 2 were passed in.");
    }

    @Test
    public void testEvaluateAllElementsTrue() throws Exception {
        assertThat(Expression.evaluate(json("$allElementsTrue: [ [ true, 1, 'someString' ] ]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$allElementsTrue: [ [ [ false ] ] ]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$allElementsTrue: [ [ ] ]"), json(""))).isEqualTo(true);
        assertThat(Expression.evaluate(json("$allElementsTrue: [ [ null, false, 0 ] ]"), json(""))).isEqualTo(false);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$allElementsTrue: null"), json("a: 'abc'")))
            .withMessage("$allElementsTrue's argument must be an array, but is null");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$allElementsTrue: [null]"), json("a: 'abc'")))
            .withMessage("$allElementsTrue's argument must be an array, but is null");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$allElementsTrue: '$a'"), json("a: 'abc'")))
            .withMessage("$allElementsTrue's argument must be an array, but is java.lang.String");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$allElementsTrue: [1, 2]"), json("a: 'abc'")))
            .withMessage("Expression $allElementsTrue takes exactly 1 arguments. 2 were passed in.");
    }

    @Test
    public void testEvaluateCeil() throws Exception {
        assertThat(Expression.evaluate(json("$ceil: '$a'"), json("a: 2.5"))).isEqualTo(3);
        assertThat(Expression.evaluate(json("$ceil: 42"), json(""))).isEqualTo(42);
        assertThat(Expression.evaluate(json("$ceil: 42.3"), json(""))).isEqualTo(43);
        assertThat(Expression.evaluate(new Document("$ceil", (double) Long.MAX_VALUE), json("{}"))).isEqualTo(Long.MAX_VALUE);
        assertThat(Expression.evaluate(new Document("$ceil", (double) Long.MIN_VALUE), json("{}"))).isEqualTo(Long.MIN_VALUE);
        assertThat(Expression.evaluate(json("$ceil: null"), json("{}"))).isNull();
        assertThat(Expression.evaluate(json("ceil: {$ceil: '$a'}"), json("a: -25.5"))).isEqualTo(json("ceil: -25"));

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$ceil: 'abc'"), json("{}")))
            .withMessage("$ceil only supports numeric types, not class java.lang.String");
    }

    @Test
    public void testEvaluateSum() throws Exception {
        assertThat(Expression.evaluate(json("$sum: null"), json("{}"))).isEqualTo(0);
        assertThat(Expression.evaluate(json("$sum: ''"), json("{}"))).isEqualTo(0);
        assertThat(Expression.evaluate(json("$sum: 5"), json("{}"))).isEqualTo(5);
        assertThat(Expression.evaluate(json("$sum: [1, 'foo', 2]"), json("{}"))).isEqualTo(3);
        assertThat(Expression.evaluate(json("$sum: ['$a', '$b']"), json("a: 7, b: 5"))).isEqualTo(12);
        assertThat(Expression.evaluate(json("$sum: []"), json("{}"))).isEqualTo(0);
    }

    @Test
    public void testEvaluateSubtract() throws Exception {
        assertThat(Expression.evaluate(json("$subtract: ['$a', '$b']"), json("a: 7, b: 5"))).isEqualTo(2);
        assertThat(Expression.evaluate(json("$subtract: [7.5, 3]"), json("{}"))).isEqualTo(4.5);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$subtract: []"), json("{}")))
            .withMessage("Expression $subtract takes exactly 2 arguments. 0 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$subtract: [1]"), json("{}")))
            .withMessage("Expression $subtract takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$subtract: 123"), json("{}")))
            .withMessage("Expression $subtract takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$subtract: ['a', 'b']"), json("{}")))
            .withMessage("cant $subtract a java.lang.String from a java.lang.String");
    }

    @Test
    public void testEvaluateYear() throws Exception {
        assertThat(Expression.evaluate(json("$year: '$a'"), json("{}"))).isNull();
        assertThat(Expression.evaluate(json("$year: '$a'"), new Document("a", toDate("2018-07-03T14:00:00Z")))).isEqualTo(2018);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$year: '$a'"), json("a: 'abc'")))
            .withMessage("can't convert from class java.lang.String to Date");
    }

    @Test
    public void testEvaluateDayOfYear() throws Exception {
        assertThat(Expression.evaluate(json("$dayOfYear: '$a'"), json("{}"))).isNull();
        assertThat(Expression.evaluate(json("$dayOfYear: '$a'"), new Document("a", toDate("2018-01-01T14:00:00Z")))).isEqualTo(1);
        assertThat(Expression.evaluate(json("$dayOfYear: '$a'"), new Document("a", toDate("2014-02-03T14:00:00Z")))).isEqualTo(34);

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$dayOfYear: '$a'"), json("a: 'abc'")))
            .withMessage("can't convert from class java.lang.String to Date");
    }

    @Test
    public void testEvaluateLiteral() throws Exception {
        assertThat(Expression.evaluate(json("$literal: { $add: [ 2, 3 ] }"), json(""))).isEqualTo(json("'$add' : [ 2, 3 ]"));
        assertThat(Expression.evaluate(json("$literal:  { $literal: 1 }"), json(""))).isEqualTo(json("'$literal' : 1"));
    }

    @Test
    public void testEvaluateIllegalExpression() throws Exception {
        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(json("$foo: '$a'"), json("{}")))
            .withMessage("Unrecognized expression '$foo'");
    }

    private static Date toDate(String instant) {
        return Date.from(Instant.parse(instant));
    }

}