package de.bwaldvogel.mongo.backend.aggregation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

import org.junit.Test;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class ExpressionTest {

    @Test
    public void testEvaluateSimpleValue() throws Exception {
        assertThat(Expression.evaluate(1, new Document())).isEqualTo(1);
        assertThat(Expression.evaluate(null, new Document())).isNull();
        assertThat(Expression.evaluate("abc", new Document())).isEqualTo("abc");
        assertThat(Expression.evaluate("$a", new Document("a", 123))).isEqualTo(123);
        assertThat(Expression.evaluate("$a", new Document())).isNull();
        assertThat(Expression.evaluate(new Document("a", 1).append("b", 2), new Document("a", -2))).isEqualTo(new Document("a", 1).append("b", 2));
    }

    @Test
    public void testEvaluateAbs() throws Exception {
        assertThat(Expression.evaluate(new Document("$abs", "$a"), new Document("a", -2))).isEqualTo(2);
        assertThat(Expression.evaluate(new Document("$abs", "$a"), new Document("a", -2.5))).isEqualTo(2.5);
        assertThat(Expression.evaluate(new Document("$abs", 123L), new Document())).isEqualTo(123L);
        assertThat(Expression.evaluate(new Document("$abs", null), new Document())).isNull();
        assertThat(Expression.evaluate(new Document("abs", new Document("$abs", "$a")), new Document("a", -25L))).isEqualTo(new Document("abs", 25L));
    }

    @Test
    public void testEvaluateSum() throws Exception {
        assertThat(Expression.evaluate(new Document("$sum", null), new Document())).isEqualTo(0);
        assertThat(Expression.evaluate(new Document("$sum", ""), new Document())).isEqualTo(0);
        assertThat(Expression.evaluate(new Document("$sum", 5), new Document())).isEqualTo(5);
        assertThat(Expression.evaluate(new Document("$sum", Arrays.asList(1, "foo", 2)), new Document())).isEqualTo(3);
        assertThat(Expression.evaluate(new Document("$sum", Arrays.asList("$a", "$b")), new Document("a", 7).append("b", 5))).isEqualTo(12);
        assertThat(Expression.evaluate(new Document("$sum", Collections.emptyList()), new Document())).isEqualTo(0);
    }

    @Test
    public void testEvaluateSubtract() throws Exception {
        assertThat(Expression.evaluate(new Document("$subtract", Arrays.asList("$a", "$b")), new Document("a", 7).append("b", 5))).isEqualTo(2);
        assertThat(Expression.evaluate(new Document("$subtract", Arrays.asList(7.5, 3)), new Document())).isEqualTo(4.5);
    }

    @Test
    public void testEvaluateYear() throws Exception {
        assertThat(Expression.evaluate(new Document("$year", "$a"), new Document())).isNull();
        assertThat(Expression.evaluate(new Document("$year", "$a"), new Document("a", toDate("2018-07-03T14:00:00Z")))).isEqualTo(2018);
    }

    @Test
    public void testEvaluateDayOfYear() throws Exception {
        assertThat(Expression.evaluate(new Document("$dayOfYear", "$a"), new Document())).isNull();
        assertThat(Expression.evaluate(new Document("$dayOfYear", "$a"), new Document("a", toDate("2018-01-01T14:00:00Z")))).isEqualTo(1);
        assertThat(Expression.evaluate(new Document("$dayOfYear", "$a"), new Document("a", toDate("2014-02-03T14:00:00Z")))).isEqualTo(34);
    }

    @Test
    public void testEvaluateIllegalExpression() throws Exception {
        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(new Document("$foo", "$a"), new Document()))
            .withMessage("Unrecognized expression '$foo'");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(new Document("$abs", "$a").append("$ceil", "$b"), new Document()))
            .withMessage("An object representing an expression must have exactly one field: {$abs=$a, $ceil=$b}");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(new Document("$abs", "abc"), new Document()))
            .withMessage("$abs only supports numeric types, not class java.lang.String");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(new Document("$subtract", Collections.emptyList()), new Document()))
            .withMessage("Expression $subtract takes exactly 2 arguments. 0 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(new Document("$subtract", Collections.singletonList(1)), new Document()))
            .withMessage("Expression $subtract takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(new Document("$subtract", 123), new Document()))
            .withMessage("Expression $subtract takes exactly 2 arguments. 1 were passed in.");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(new Document("$subtract", Arrays.asList("a", "b")), new Document()))
            .withMessage("cant $subtract a java.lang.String from a java.lang.String");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(new Document("$year", "$a"), new Document("a", "abc")))
            .withMessage("can't convert from class java.lang.String to Date");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Expression.evaluate(new Document("$dayOfYear", "$a"), new Document("a", "abc")))
            .withMessage("can't convert from class java.lang.String to Date");
    }

    private static Date toDate(String instant) {
        return Date.from(Instant.parse(instant));
    }

}