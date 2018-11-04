package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.backend.TestUtils.json;
import static de.bwaldvogel.mongo.backend.TestUtils.toArray;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.bson.Document;
import org.junit.Test;

import com.mongodb.MongoCommandException;

public abstract class AbstractAggregationTest extends AbstractTest {

    @Test
    public void testUnrecognizedAggregatePipelineStage() throws Exception {
        List<Document> pipeline = Collections.singletonList(json("$unknown: {}"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("Command failed with error 40324: 'Unrecognized pipeline stage name: '$unknown'");
    }

    @Test
    public void testIllegalAggregatePipelineStage() throws Exception {
        List<Document> pipeline = Collections.singletonList(json("$unknown: {}, bar: 1"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("Command failed with error 40323: 'A pipeline stage specification object must contain exactly one field.'");
    }

    @Test
    public void testAggregateWithMissingCursor() throws Exception {
        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> db.runCommand(json("aggregate: 'collection', pipeline: [{$match: {}}]")))
            .withMessageContaining("Command failed with error 9: 'The 'cursor' option is required, except for aggregate with the explain argument'");
    }

    @Test
    public void testAggregateWithComplexGroupBySumPipeline() throws Exception {
        Document query = new Document("_id", null);
        query.putAll(json("n: {$sum: 1}, sumOfA: {$sum: '$a'}, sumOfB: {$sum: '$b.value'}"));
        List<Document> pipeline = Collections.singletonList(new Document("$group", query));

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id:1, a:30, b: {value: 20}"));
        collection.insertOne(json("_id:2, a:15, b: {value: 10.5}"));
        collection.insertOne(json("_id:3, b: {value: 1}"));
        collection.insertOne(json("_id:4, a: {value: 5}"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(new Document("_id", null)
                .append("n", 4)
                .append("sumOfA", 45)
                .append("sumOfB", 31.5));
    }

    @Test
    public void testAggregateWithGroupByMinAndMax() throws Exception {
        Document query = new Document("_id", null);
        query.putAll(json("minA: {$min: '$a'}, maxB: {$max: '$b.value'}, maxC: {$max: '$c'}, minC: {$min: '$c'}"));
        List<Document> pipeline = Collections.singletonList(new Document("$group", query));

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id:1, a:30, b: {value: 20}, c: 1.0"));
        collection.insertOne(json("_id:2, a:15, b: {value: 10}, c: 2"));
        collection.insertOne(json("_id:3, c: 'zzz'"));
        collection.insertOne(json("_id:4, c: 'aaa'"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(new Document("_id", null)
                .append("minA", 15)
                .append("maxB", 20)
                .append("minC", 1.0)
                .append("maxC", "zzz"));
    }

    @Test
    public void testAggregateWithGroupByNonExistingMinAndMax() throws Exception {
        Document query = new Document("_id", null);
        query.putAll(json("minOfA: {$min: '$doesNotExist'}, maxOfB: {$max: '$doesNotExist'}"));
        List<Document> pipeline = Collections.singletonList(new Document("$group", query));

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id:1, a:30, b: {value: 20}"));
        collection.insertOne(json("_id:2, a:15, b: {value: 10}"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(new Document("_id", null)
                .append("minOfA", null)
                .append("maxOfB", null));
    }

    @Test
    public void testAggregateWithUnknownGroupOperator() throws Exception {
        Document query = new Document("_id", null);
        query.putAll(json("n: {$foo: 1}"));
        List<Document> pipeline = Collections.singletonList(new Document("$group", query));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("Command failed with error 15952: 'unknown group operator '$foo''");
    }

    @Test
    public void testAggregateWithTooManyGroupOperators() throws Exception {
        Document query = new Document("_id", null);
        query.putAll(json("n: {$sum: 1, $max: 1}"));
        List<Document> pipeline = Collections.singletonList(new Document("$group", query));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("Command failed with error 40238: 'The field 'n' must specify one accumulator'");
    }

    @Test
    public void testAggregateWithEmptyPipeline() throws Exception {
        assertThat(toArray(collection.aggregate(Collections.emptyList()))).isEmpty();

        collection.insertOne(json("_id:1"));
        collection.insertOne(json("_id:2"));

        assertThat(toArray(collection.aggregate(Collections.emptyList())))
            .containsExactly(json("_id:1"), json("_id:2"));
    }

    @Test
    public void testAggregateWithMissingIdInGroupSpecification() throws Exception {
        List<Document> pipeline = Collections.singletonList(new Document("$group", json("n: {$sum: 1}")));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> toArray(collection.aggregate(pipeline)))
            .withMessageContaining("Command failed with error 15955: 'a group specification must include an _id'");
    }

    @Test
    public void testAggregateWithGroupBySumPipeline() throws Exception {
        Document query = new Document("_id", null);
        query.putAll(json("n: {$sum: 1}"));
        List<Document> pipeline = Collections.singletonList(new Document("$group", query));

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id:1"));
        collection.insertOne(json("_id:2"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(new Document("_id", null).append("n", 2));

        query.putAll(json("n: {$sum: 'abc'}"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(new Document("_id", null).append("n", 0));

        query.putAll(json("n: {$sum: 2}"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(new Document("_id", null).append("n", 4));

        query.putAll(json("n: {$sum: 1.75}"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(new Document("_id", null).append("n", 3.5));

        query.putAll(new Document("n", new Document("$sum", 10000000000L)));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(new Document("_id", null).append("n", 20000000000L));

        query.putAll(new Document("n", new Document("$sum", -2.5F)));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(new Document("_id", null).append("n", -5.0));
    }

    @Test
    public void testAggregateWithGroupByAvg() throws Exception {
        Document query = new Document("_id", null);
        query.putAll(json("avg: {$avg: 1}"));
        List<Document> pipeline = Collections.singletonList(new Document("$group", query));

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id:1, a: 6.0, b: 'zzz'"));
        collection.insertOne(json("_id:2, a: 3.0, b: 'aaa'"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(new Document("_id", null).append("avg", 1.0));

        query.putAll(json("avg: {$avg: '$a'}, avgB: {$avg: '$b'}"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(new Document("_id", null).append("avg", 4.5).append("avgB", null));
    }

    @Test
    public void testAggregateWithGroupByKey() throws Exception {
        List<Document> pipeline = Collections.singletonList(json("$group: {_id: '$a', count: {$sum: 1}, avg: {$avg: '$b'}}"));

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id:1, a: 1"));
        collection.insertOne(json("_id:2, a: 1"));
        collection.insertOne(json("_id:3, a: 2, b: 3"));
        collection.insertOne(json("_id:4, a: 2, b: 4"));
        collection.insertOne(json("_id:5, a: 5, b: 10"));
        collection.insertOne(json("_id:6, a: 7, c: 'a'"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(
                json("_id: 1, count: 2").append("avg", null),
                json("_id: 2, count: 2, avg: 3.5"),
                json("_id: 5, count: 1, avg: 10.0"),
                json("_id: 7, count: 1").append("avg", null)
            );
    }

    @Test
    public void testAggregateWithSimpleExpressions() throws Exception {
        Document query = json("$group: {_id: {$abs: '$value'}, count: {$sum: 1}}");
        List<Document> pipeline = Collections.singletonList(query);

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id: 1, value: 1"));
        collection.insertOne(json("_id: 2, value: -1"));
        collection.insertOne(json("_id: 3, value: 2"));
        collection.insertOne(json("_id: 4, value: 2"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(
                json("_id: 1, count: 2"),
                json("_id: 2, count: 2")
            );
    }

    @Test
    public void testAggregateWithMultipleExpressionsInKey() throws Exception {
        Document query = json("$group: {_id: {abs: {$abs: '$value'}, sum: {$subtract: ['$end', '$start']}}, count: {$sum: 1}}");
        List<Document> pipeline = Collections.singletonList(query);

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id: 1, value: 1, start: 5, end: 8"));
        collection.insertOne(json("_id: 2, value: -1, start: 4, end: 4"));
        collection.insertOne(json("_id: 3, value: 2, start: 9, end: 7"));
        collection.insertOne(json("_id: 4, value: 2, start: 6, end: 7"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(
                json("_id: {abs: 1, sum: 3}, count: 1"),
                json("_id: {abs: 1, sum: 0}, count: 1"),
                json("_id: {abs: 2, sum: -2}, count: 1"),
                json("_id: {abs: 2, sum: 1}, count: 1")
            );
    }

    @Test
    public void testAggregateWithAddToSet() throws Exception {
        Document query = json("$group: {_id: { day: { $dayOfYear: '$date'}, year: { $year: '$date' } }, itemsSold: { $addToSet: '$item' }}");
        List<Document> pipeline = Collections.singletonList(query);

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id: 1, item: 'abc', price: 10, quantity:  2").append("date", Instant.parse("2014-01-01T08:00:00Z")));
        collection.insertOne(json("_id: 2, item: 'jkl', price: 20, quantity:  1").append("date", Instant.parse("2014-02-03T09:00:00Z")));
        collection.insertOne(json("_id: 3, item: 'xyz', price:  5, quantity:  5").append("date", Instant.parse("2014-02-03T09:05:00Z")));
        collection.insertOne(json("_id: 4, item: 'abc', price: 10, quantity: 10").append("date", Instant.parse("2014-02-15T08:00:00Z")));
        collection.insertOne(json("_id: 5, item: 'xyz', price:  5, quantity: 10").append("date", Instant.parse("2014-02-15T09:12:00Z")));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(
                json("_id: { day:  1, year: 2014 }, itemsSold: [ 'abc' ]"),
                json("_id: { day: 34, year: 2014 }, itemsSold: [ 'jkl', 'xyz' ]"),
                json("_id: { day: 46, year: 2014 }, itemsSold: [ 'abc', 'xyz' ]")
            );
    }

    @Test
    public void testAggregateWithEmptyAddToSet() throws Exception {
        Document query = json("$group: {_id: 1, set: { $addToSet: '$foo' }}");
        List<Document> pipeline = Collections.singletonList(query);

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(json("_id: 1, set: [ ]"));
    }

    @Test
    public void testAggregateWithAdd() throws Exception {
        Document query = json("$project: { item: 1, total: { $add: [ '$price', '$fee' ] } }");
        List<Document> pipeline = Collections.singletonList(query);

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id: 1, item: 'abc', price: 10, fee: 2"));
        collection.insertOne(json("_id: 2, item: 'jkl', price: 20, fee: 1"));
        collection.insertOne(json("_id: 3, item: 'xyz', price: 5, fee: 0"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(
                json("_id: 1, item: 'abc', total: 12"),
                json("_id: 2, item: 'jkl', total: 21"),
                json("_id: 3, item: 'xyz', total: 5 ")
            );
    }

    @Test
    public void testAggregateWithSort() throws Exception {
        Document query = json("$sort: { price: -1, fee: 1 }");
        List<Document> pipeline = Collections.singletonList(query);

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id: 1, price: 10, fee: 1"));
        collection.insertOne(json("_id: 2, price: 20, fee: 0"));
        collection.insertOne(json("_id: 3, price: 10, fee: 0"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(
                json("_id: 2, price: 20, fee: 0"),
                json("_id: 3, price: 10, fee: 0"),
                json("_id: 1, price: 10, fee: 1")
            );
    }

    @Test
    public void testAggregateWithProjection() throws Exception {
        Document query = json("$project: {_id: 1, value: '$x', n: '$foo.bar', other: null}");
        List<Document> pipeline = Collections.singletonList(query);

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id: 1, x: 10, foo: 'abc'"));
        collection.insertOne(json("_id: 2, x: 20"));
        collection.insertOne(json("_id: 3, x: 30, foo: {bar: 7.3}"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(
                json("_id: 1, value: 10, other: null"),
                json("_id: 2, value: 20, other: null"),
                json("_id: 3, value: 30, n: 7.3, other: null")
            );
    }

    @Test
    public void testAggregateWithAddFields() throws Exception {
        Document query = json("$addFields: {value: '$x'}");
        List<Document> pipeline = Collections.singletonList(query);

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id: 1, x: 10"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3, value: 123"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(
                json("_id: 1, x: 10, value: 10"),
                json("_id: 2, value: null"),
                json("_id: 3, value: null")
            );
    }

    @Test
    public void testAggregateWithMultipleMatches() throws Exception {
        Document match1 = json("$match: {price: {$lt: 100}}");
        Document match2 = json("$match: {quality: {$gt: 10}}");
        List<Document> pipeline = Arrays.asList(match1, match2);

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id: 1, price: 10, quality: 50"));
        collection.insertOne(json("_id: 2, price: 150, quality: 500"));
        collection.insertOne(json("_id: 3, price: 50, quality: 150"));
        collection.insertOne(json("_id: 4, price: 10, quality: 5"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(
                json("_id: 1, price: 10, quality: 50"),
                json("_id: 3, price: 50, quality: 150")
            );
    }

    @Test
    public void testAggregateWithCount() throws Exception {
        Document match = json("$match: {score: {$gt: 80}}");
        Document count = json("$count: 'passing_scores'");
        List<Document> pipeline = Arrays.asList(match, count);

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id: 1, subject: 'History', score: 88"));
        collection.insertOne(json("_id: 2, subject: 'History', score: 92"));
        collection.insertOne(json("_id: 3, subject: 'History', score: 97"));
        collection.insertOne(json("_id: 4, subject: 'History', score: 71"));
        collection.insertOne(json("_id: 5, subject: 'History', score: 79"));
        collection.insertOne(json("_id: 6, subject: 'History', score: 83"));

        assertThat(toArray(collection.aggregate(pipeline))).containsExactly(json("passing_scores: 4"));
    }

}
