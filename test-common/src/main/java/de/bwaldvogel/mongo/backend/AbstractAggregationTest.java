package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.backend.TestUtils.json;
import static de.bwaldvogel.mongo.backend.TestUtils.toArray;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Test;

import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoCollection;

public abstract class AbstractAggregationTest extends AbstractTest {

    @Test
    public void testUnrecognizedAggregatePipelineStage() throws Exception {
        List<Document> pipeline = Collections.singletonList(json("$unknown: {}"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("Command failed with error 40324 (Location40324): 'Unrecognized pipeline stage name: '$unknown'");
    }

    @Test
    public void testIllegalAggregatePipelineStage() throws Exception {
        List<Document> pipeline = Collections.singletonList(json("$unknown: {}, bar: 1"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("Command failed with error 40323 (Location40323): 'A pipeline stage specification object must contain exactly one field.'");
    }

    @Test
    public void testAggregateWithMissingCursor() throws Exception {
        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> db.runCommand(json("aggregate: 'collection', pipeline: [{$match: {}}]")))
            .withMessageContaining("Command failed with error 9 (FailedToParse): 'The 'cursor' option is required, except for aggregate with the explain argument'");
    }

    @Test
    public void testAggregateWithComplexGroupBySumPipeline() throws Exception {
        Document query = json("_id: null, n: {$sum: 1}, sumOfA: {$sum: '$a'}, sumOfB: {$sum: '$b.value'}");
        List<Document> pipeline = Collections.singletonList(new Document("$group", query));

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id: 1, a:30, b: {value: 20}"));
        collection.insertOne(json("_id: 2, a:15, b: {value: 10.5}"));
        collection.insertOne(json("_id: 3, b: {value: 1}"));
        collection.insertOne(json("_id: 4, a: {value: 5}"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(json("_id: null, n:4, sumOfA: 45, sumOfB: 31.5"));
    }

    @Test
    public void testAggregateWithGroupByMinAndMax() throws Exception {
        Document query = json("_id: null, minA: {$min: '$a'}, maxB: {$max: '$b.value'}, maxC: {$max: '$c'}, minC: {$min: '$c'}");
        List<Document> pipeline = Collections.singletonList(new Document("$group", query));

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id: 1, a:30, b: {value: 20}, c: 1.0"));
        collection.insertOne(json("_id: 2, a:15, b: {value: 10}, c: 2"));
        collection.insertOne(json("_id: 3, c: 'zzz'"));
        collection.insertOne(json("_id: 4, c: 'aaa'"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(json("_id: null, minA: 15, maxB: 20, minC: 1.0, maxC: 'zzz'"));
    }

    @Test
    public void testAggregateWithGroupByNonExistingMinAndMax() throws Exception {
        Document query = json("_id: null, minOfA: {$min: '$doesNotExist'}, maxOfB: {$max: '$doesNotExist'}");
        List<Document> pipeline = Collections.singletonList(new Document("$group", query));

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id: 1, a: 30, b: {value: 20}"));
        collection.insertOne(json("_id: 2, a: 15, b: {value: 10}"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(json("_id: null, minOfA: null, maxOfB: null"));
    }

    @Test
    public void testAggregateWithUnknownGroupOperator() throws Exception {
        Document query = json("_id: null, n: {$foo: 1}");
        List<Document> pipeline = Collections.singletonList(new Document("$group", query));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("Command failed with error 15952 (Location15952): 'unknown group operator '$foo''");
    }

    @Test
    public void testAggregateWithTooManyGroupOperators() throws Exception {
        Document query = json("_id: null, n: {$sum: 1, $max: 1}");
        List<Document> pipeline = Collections.singletonList(new Document("$group", query));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("Command failed with error 40238 (Location40238): 'The field 'n' must specify one accumulator'");
    }

    @Test
    public void testAggregateWithEmptyPipeline() throws Exception {
        assertThat(toArray(collection.aggregate(Collections.emptyList()))).isEmpty();

        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));

        assertThat(toArray(collection.aggregate(Collections.emptyList())))
            .containsExactly(json("_id: 1"), json("_id: 2"));
    }

    @Test
    public void testAggregateWithMissingIdInGroupSpecification() throws Exception {
        List<Document> pipeline = Collections.singletonList(new Document("$group", json("n: {$sum: 1}")));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> toArray(collection.aggregate(pipeline)))
            .withMessageContaining("Command failed with error 15955 (Location15955): 'a group specification must include an _id'");
    }

    @Test
    public void testAggregateWithGroupBySumPipeline() throws Exception {
        Document query = json("_id: null, n: {$sum: 1}");
        List<Document> pipeline = Collections.singletonList(new Document("$group", query));

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(json("_id: null, n: 2"));

        query.putAll(json("n: {$sum: 'abc'}"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(json("_id: null, n: 0"));

        query.putAll(json("n: {$sum: 2}"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(json("_id: null, n: 4"));

        query.putAll(json("n: {$sum: 1.75}"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(json("_id: null, n: 3.5"));

        query.putAll(new Document("n", new Document("$sum", 10000000000L)));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(json("_id: null, n: 20000000000"));

        query.putAll(new Document("n", new Document("$sum", -2.5F)));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(json("_id: null, n: -5.0"));
    }

    @Test
    public void testAggregateWithGroupByAvg() throws Exception {
        Document query = json("_id: null, avg: {$avg: 1}");
        List<Document> pipeline = Collections.singletonList(new Document("$group", query));

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id: 1, a: 6.0, b: 'zzz'"));
        collection.insertOne(json("_id: 2, a: 3.0, b: 'aaa'"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(json("_id: null, avg: 1.0"));

        query.putAll(json("avg: {$avg: '$a'}, avgB: {$avg: '$b'}"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(json("_id: null, avg: 4.5, avgB: null"));
    }

    @Test
    public void testAggregateWithGroupByKey() throws Exception {
        List<Document> pipeline = Collections.singletonList(json("$group: {_id: '$a', count: {$sum: 1}, avg: {$avg: '$b'}}"));

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id: 1, a: 1"));
        collection.insertOne(json("_id: 2, a: 1"));
        collection.insertOne(json("_id: 3, a: 2, b: 3"));
        collection.insertOne(json("_id: 4, a: 2, b: 4"));
        collection.insertOne(json("_id: 5, a: 5, b: 10"));
        collection.insertOne(json("_id: 6, a: 7, c: 'a'"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactlyInAnyOrder(
                json("_id: 1, count: 2, avg: null"),
                json("_id: 2, count: 2, avg: 3.5"),
                json("_id: 5, count: 1, avg: 10.0"),
                json("_id: 7, count: 1, avg: null")
            );
    }

    @Test
    public void testAggregateWithGroupByNumberEdgeCases() throws Exception {
        String groupBy = "$group: {_id: '$a', count: {$sum: 1}, avg: {$avg: '$a'}, min: {$min: '$a'}, max: {$max: '$a'}}";
        List<Document> pipeline = Collections.singletonList(json(groupBy));

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id: 1, a: 1.0"));
        collection.insertOne(json("_id: 2, a: 1"));
        collection.insertOne(json("_id: 3, a: -0.0"));
        collection.insertOne(json("_id: 4, a: 0.0"));
        collection.insertOne(json("_id: 5, a: 0"));
        collection.insertOne(json("_id: 6, a: 1.5"));
        collection.insertOne(json("_id: 7, a: null"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactlyInAnyOrder(
                json("_id: -0.0, count: 3, avg: 0.0, min: -0.0, max: -0.0"),
                json("_id: 1.0, count: 2, avg: 1.0, min: 1.0, max: 1.0"),
                json("_id: 1.5, count: 1, avg: 1.5, min: 1.5, max: 1.5"),
                json("_id: null, count: 1, avg: null, min: null, max: null")
            );
    }

    @Test
    public void testAggregateWithGroupByDocuments() throws Exception {
        String groupBy = "$group: {_id: '$a', count: {$sum: 1}}";
        String sort = "$sort: {_id: 1}";
        List<Document> pipeline = Arrays.asList(json(groupBy), json(sort));

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id:  1, a: 1.0"));
        collection.insertOne(json("_id:  2, a: {b: 1}"));
        collection.insertOne(json("_id:  3, a: {b: 1.0}"));
        collection.insertOne(json("_id:  4, a: {b: 1, c: 1}"));
        collection.insertOne(json("_id:  5, a: {b: {c: 1}}"));
        collection.insertOne(json("_id:  6, a: {b: {c: 1.0}}"));
        collection.insertOne(json("_id:  7, a: {b: {c: 1.0, d: 1}}"));
        collection.insertOne(json("_id:  8, a: {b: {d: 1, c: 1.0}}"));
        collection.insertOne(json("_id:  9, a: {c: 1, b: 1}"));
        collection.insertOne(json("_id: 10, a: null"));
        collection.insertOne(json("_id: 11, a: {b: 1, c: 1}"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(
                json("_id: null, count: 1"),
                json("_id: 1.0, count: 1"),
                json("_id: {b: 1}, count: 2"),
                json("_id: {b: 1, c: 1}, count: 2"),
                json("_id: {c: 1, b: 1}, count: 1"),
                json("_id: {b: {c: 1}}, count: 2"),
                json("_id: {b: {c: 1.0, d: 1}}, count: 1"),
                json("_id: {b: {d: 1, c: 1.0}}, count: 1")
            );
    }

    @Test
    public void testAggregateWithSimpleExpressions() throws Exception {
        Document query = json("$group: {_id: {$abs: '$value'}, count: {$sum: 1}}");
        List<Document> pipeline = Collections.singletonList(query);

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id: 1, value: 1"));
        collection.insertOne(json("_id: -2, value: -1"));
        collection.insertOne(json("_id: 3, value: 2"));
        collection.insertOne(json("_id: 4, value: 2"));
        collection.insertOne(json("_id: 5, value: -2.5"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactlyInAnyOrder(
                json("_id: 1, count: 2"),
                json("_id: 2, count: 2"),
                json("_id: 2.5, count: 1")
            );
    }

    @Test
    public void testAggregateWithMultipleExpressionsInKey() throws Exception {
        Document query = json("$group: {_id: {abs: {$abs: '$value'}, sum: {$subtract: ['$end', '$start']}}, count: {$sum: 1}}");
        List<Document> pipeline = Collections.singletonList(query);

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id: 1, value: NaN"));
        collection.insertOne(json("_id: 2, value: 1, start: 5, end: 8"));
        collection.insertOne(json("_id: 3, value: -1, start: 4, end: 4"));
        collection.insertOne(json("_id: 4, value: 2, start: 9, end: 7"));
        collection.insertOne(json("_id: 5, value: 2, start: 6, end: 7"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactlyInAnyOrder(
                json("_id: {abs: NaN, sum: null}, count: 1"),
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

        collection.insertOne(json("_id: 1, item: 'zzz', price:  5, quantity: 10").append("date", date("2014-02-15T09:12:00Z")));
        collection.insertOne(json("_id: 2, item: 'abc', price: 10, quantity:  2").append("date", date("2014-01-01T08:00:00Z")));
        collection.insertOne(json("_id: 3, item: 'jkl', price: 20, quantity:  1").append("date", date("2014-02-03T09:00:00Z")));
        collection.insertOne(json("_id: 4, item: 'xyz', price:  5, quantity:  5").append("date", date("2014-02-03T09:05:00Z")));
        collection.insertOne(json("_id: 5, item: 'abc', price: 10, quantity: 10").append("date", date("2014-02-15T08:00:00Z")));
        collection.insertOne(json("_id: 6, item: 'xyz', price:  5, quantity: 10").append("date", date("2014-02-15T09:12:00Z")));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactlyInAnyOrder(
                json("_id: { day:  1, year: 2014 }, itemsSold: [ 'abc' ]"),
                json("_id: { day: 34, year: 2014 }, itemsSold: [ 'xyz', 'jkl' ]"),
                json("_id: { day: 46, year: 2014 }, itemsSold: [ 'xyz', 'abc', 'zzz' ]")
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
    public void testAggregateWithExpressionProjection() throws Exception {
        Document query = json("$project: {_id: 0, idHex: {$toString: '$_id'}}");
        List<Document> pipeline = Collections.singletonList(query);

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(new Document("_id", new ObjectId("abcd01234567890123456789")));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(json("idHex: 'abcd01234567890123456789'"));
    }

    @Test
    public void testAggregateWithAddFields() throws Exception {
        Document addFields = json("$addFields: {value: '$x'}");
        List<Document> pipeline = Collections.singletonList(addFields);

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id: 1, x: 10"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3, value: 123"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(
                json("_id: 1, x: 10, value: 10"),
                json("_id: 2"),
                json("_id: 3")
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
    public void testAggregateWithCeil() throws Exception {
        Document query = json("$project: {a: 1, ceil: {$ceil: '$a'}}");
        List<Document> pipeline = Collections.singletonList(query);

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id: 1, a: 9.25"));
        collection.insertOne(json("_id: 2, a: 8.73"));
        collection.insertOne(json("_id: 3, a: 4.32"));
        collection.insertOne(json("_id: 4, a: -5.34"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactlyInAnyOrder(
                json("_id: 1, a: 9.25, ceil: 10.0"),
                json("_id: 2, a: 8.73, ceil: 9.0"),
                json("_id: 3, a: 4.32, ceil: 5.0"),
                json("_id: 4, a: -5.34, ceil: -5.0")
            );
    }

    @Test
    public void testAggregateWithNumericOperators() throws Exception {
        Document query = json("$project: {a: 1, exp: {$exp: '$a'}, ln: {$ln: '$a'}, log10: {$log10: '$a'}, sqrt: {$sqrt: '$a'}}");
        List<Document> pipeline = Collections.singletonList(query);

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id: 1, a: 1"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(json("_id: 1, a: 1, exp: 2.718281828459045, ln: 0.0, log10: 0.0, sqrt: 1.0"));
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

    @Test
    public void testAggregateWithFirstAndLast() throws Exception {
        Document sort = json("$sort: { item: 1, date: 1 }");
        Document group = json("$group: {_id: '$item', firstSale: { $first: '$date' }, lastSale: { $last: '$date'} }");
        List<Document> pipeline = Arrays.asList(sort, group);

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id: 1, item: 'abc', price: 10, quantity:  2").append("date", date("2014-01-01T08:00:00Z")));
        collection.insertOne(json("_id: 2, item: 'jkl', price: 20, quantity:  1").append("date", date("2014-02-03T09:00:00Z")));
        collection.insertOne(json("_id: 3, item: 'xyz', price:  5, quantity:  5").append("date", date("2014-02-03T09:05:00Z")));
        collection.insertOne(json("_id: 4, item: 'abc', price: 10, quantity: 10").append("date", date("2014-02-15T08:00:00Z")));
        collection.insertOne(json("_id: 5, item: 'xyz', price:  5, quantity: 10").append("date", date("2014-02-15T09:12:00Z")));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactlyInAnyOrder(
                json("_id: 'abc'").append("firstSale", date("2014-01-01T08:00:00Z")).append("lastSale", date("2014-02-15T08:00:00Z")),
                json("_id: 'jkl'").append("firstSale", date("2014-02-03T09:00:00Z")).append("lastSale", date("2014-02-03T09:00:00Z")),
                json("_id: 'xyz'").append("firstSale", date("2014-02-03T09:05:00Z")).append("lastSale", date("2014-02-15T09:12:00Z"))
            );
    }

    @Test
    public void testAggregateWithPush() throws Exception {
        Document group = json("$group: {_id: null, a: {$push: '$a'}, b: {$push: {v: '$b'}}, c: {$push: '$c'}}");
        List<Document> pipeline = Collections.singletonList(group);

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id: 1, a: 10, b: 0.1"));
        collection.insertOne(json("_id: 2, a: 20, b: 0.2"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(json("_id: null, a: [10, 20], b: [{v: 0.1}, {v: 0.2}], c: []"));
    }

    @Test
    public void testAggregateWithUndefinedVariable() throws Exception {
        Document project = json("$project: {result: '$$UNDEFINED'}");
        List<Document> pipeline = Collections.singletonList(project);

        collection.insertOne(json("_id: 1"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("Command failed with error 17276 (Location17276): 'Use of undefined variable: UNDEFINED'");
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/31
    @Test
    public void testAggregateWithRootVariable() throws Exception {
        Document project = json("$project: {_id: 0, doc: '$$ROOT', a: '$$ROOT.a', a_v: '$$ROOT.a.v'}");
        List<Document> pipeline = Collections.singletonList(project);

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id: 1, a: {v: 10}"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(json("doc: {_id: 1, a: {v: 10}}, a: {v: 10}, a_v: 10"));
    }

    @Test
    public void testAggregateWithSetOperations() throws Exception {
        Document project = json("$project: {union: {$setUnion: ['$a', '$b']}, diff: {$setDifference: ['$a', '$b']}, intersection: {$setIntersection: ['$a', '$b']}}");
        List<Document> pipeline = Collections.singletonList(project);

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id: 1, a: [3, 2, 1]"));
        collection.insertOne(json("_id: 2, a: [1.0, -0.0], b: [3, 2, 0]"));
        collection.insertOne(json("_id: 3, a: [{a: 0}, {a: 1}], b: [{a: 0.0}, {a: 0.5}]"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactlyInAnyOrder(
                json("_id: 1, diff: null, intersection: null, union: null"),
                json("_id: 2, diff: [1.0], intersection: [-0.0], union: [-0.0, 1.0, 2, 3]"),
                json("_id: 3, diff: [{a: 1}], intersection: [{a: 0}], union: [{a: 0}, {a: 0.5}, {a: 1}]")
            );
    }

    @Test
    public void testAggregateWithComparisonOperations() throws Exception {
        collection.insertOne(json("_id: 1, v: 'abc'"));
        collection.insertOne(json("_id: 2, v: null"));
        collection.insertOne(json("_id: 3, v: 10"));
        collection.insertOne(json("_id: 4, v: [10, 20, 30]"));
        collection.insertOne(json("_id: 5, v: ['abc']"));
        collection.insertOne(json("_id: 6, v: [30, 40]"));
        collection.insertOne(json("_id: 7, v: [5]"));

        Document project = json("$project: {cmp1: {$cmp: ['$v', 10]}, cmp2: {$cmp: ['$v', [10]]}}");
        List<Document> pipeline = Collections.singletonList(project);

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactlyInAnyOrder(
                json("_id: 1, cmp1:  1, cmp2: -1"),
                json("_id: 2, cmp1: -1, cmp2: -1"),
                json("_id: 3, cmp1:  0, cmp2: -1"),
                json("_id: 4, cmp1:  1, cmp2:  1"),
                json("_id: 5, cmp1:  1, cmp2:  1"),
                json("_id: 6, cmp1:  1, cmp2:  1"),
                json("_id: 7, cmp1:  1, cmp2: -1")
            );

        project = json("$project: {gt1: {$gt: ['$v', 10]}, gt2: {$gt: ['$v', [10]]}}");
        pipeline = Collections.singletonList(project);

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactlyInAnyOrder(
                json("_id: 1, gt1: true, gt2: false"),
                json("_id: 2, gt1: false, gt2: false"),
                json("_id: 3, gt1: false, gt2: false"),
                json("_id: 4, gt1: true, gt2: true"),
                json("_id: 5, gt1: true, gt2: true"),
                json("_id: 6, gt1: true, gt2: true"),
                json("_id: 7, gt1: true, gt2: false")
            );

        project = json("$project: {lt1: {$lt: ['$v', 10]}, lt2: {$lt: ['$v', [10]]}}");
        pipeline = Collections.singletonList(project);

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactlyInAnyOrder(
                json("_id: 1, lt1: false, lt2: true"),
                json("_id: 2, lt1: true, lt2: true"),
                json("_id: 3, lt1: false, lt2: true"),
                json("_id: 4, lt1: false, lt2: false"),
                json("_id: 5, lt1: false, lt2: false"),
                json("_id: 6, lt1: false, lt2: false"),
                json("_id: 7, lt1: false, lt2: true")
            );
    }

    @Test
    public void testAggregateWithSplit() throws Exception {
        Document project = json("$project: {_id: 1, names: {$split: ['$name', ' ']}}");
        List<Document> pipeline = Collections.singletonList(project);

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();

        collection.insertOne(json("_id: 1, name: 'first document'"));
        collection.insertOne(json("_id: 2, name: 'second document'"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(
                json("_id: 1, names: ['first', 'document']"),
                json("_id: 2, names: ['second', 'document']")
            );
    }

    @Test
    public void testAggregateWithUnwind() throws Exception {
        Document unwind = json("$unwind: '$sizes'");
        List<Document> pipeline = Collections.singletonList(unwind);

        assertThat(toArray(collection.aggregate(pipeline))).isEmpty();
        collection.insertOne(json("_id: 1, item: 'ABC1', sizes: ['S', 'M', 'L']"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(
                json("_id: 1, item: 'ABC1', sizes: 'S'"),
                json("_id: 1, item: 'ABC1', sizes: 'M'"),
                json("_id: 1, item: 'ABC1', sizes: 'L'")
            );
    }

    @Test
    public void testAggregateWithLookup() {
        MongoCollection<Document> authorsCollection = db.getCollection("authors");
        authorsCollection.insertOne(json("_id: 1, name: 'Uncle Bob'"));
        authorsCollection.insertOne(json("_id: 2, name: 'Martin Fowler'"));
        authorsCollection.insertOne(json("_id: null, name: 'Null Author'"));

        Document lookup = json("$lookup: {from: 'authors', localField: 'authorId', foreignField: '_id', as: 'author'}");
        List<Document> pipeline = Collections.singletonList(lookup);

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, title: 'Refactoring', authorId: 2"));
        collection.insertOne(json("_id: 2, title: 'Clean Code', authorId: 1"));
        collection.insertOne(json("_id: 3, title: 'Clean Coder', authorId: 1"));
        collection.insertOne(json("_id: 4, title: 'Unknown author', authorId: 3"));
        collection.insertOne(json("_id: 5, title: 'No author', authorId: null"));
        collection.insertOne(json("_id: 6, title: 'Missing author'"));

        assertThat(toArray(collection.aggregate(pipeline)))
            .containsExactly(
                json("_id: 1, title: 'Refactoring', authorId: 2, author: [{_id: 2, name: 'Martin Fowler'}]"),
                json("_id: 2, title: 'Clean Code', authorId: 1, author: [{_id: 1, name: 'Uncle Bob'}]"),
                json("_id: 3, title: 'Clean Coder', authorId: 1, author: [{_id: 1, name: 'Uncle Bob'}]"),
                json("_id: 4, title: 'Unknown author', authorId: 3, author: []"),
                json("_id: 5, title: 'No author', authorId: null, author: [{_id: null, name: 'Null Author'}]"),
                json("_id: 6, title: 'Missing author', author: [{_id: null, name: 'Null Author'}]")
            );
    }

    private static Date date(String instant) {
        return Date.from(Instant.parse(instant));
    }

}
