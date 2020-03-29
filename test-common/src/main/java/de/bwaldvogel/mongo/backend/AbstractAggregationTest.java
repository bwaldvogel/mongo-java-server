package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.backend.TestUtils.date;
import static de.bwaldvogel.mongo.backend.TestUtils.instant;
import static de.bwaldvogel.mongo.backend.TestUtils.json;
import static de.bwaldvogel.mongo.backend.TestUtils.jsonList;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.bson.BsonInvalidOperationException;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import com.mongodb.Function;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoCollection;

public abstract class AbstractAggregationTest extends AbstractTest {

    @Test
    public void testUnrecognizedAggregatePipelineStage() throws Exception {
        List<Document> pipeline = jsonList("$unknown: {}");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("Command failed with error 40324 (Location40324): 'Unrecognized pipeline stage name: '$unknown'");
    }

    @Test
    public void testIllegalAggregatePipelineStage() throws Exception {
        List<Document> pipeline = jsonList("$unknown: {}, bar: 1");

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
    public void testAggregateWithIllegalPipeline() throws Exception {
        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> db.runCommand(json("aggregate: 'collection', cursor: {}, pipeline: 123")))
            .withMessageContaining("Command failed with error 14 (TypeMismatch): ''pipeline' option must be specified as an array'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> db.runCommand(json("aggregate: 'collection', cursor: {}, pipeline: [1, 2, 3]")))
            .withMessageContaining("Command failed with error 14 (TypeMismatch): 'Each element of the 'pipeline' array must be an object");
    }

    @Test
    public void testAggregateWithComplexGroupBySumPipeline() throws Exception {
        Document query = json("_id: null, n: {$sum: 1}, sumOfA: {$sum: '$a'}, sumOfB: {$sum: '$b.value'}");
        List<Document> pipeline = Collections.singletonList(new Document("$group", query));

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, a: 30, b: {value: 20}"));
        collection.insertOne(json("_id: 2, a: 15, b: {value: 10.5}"));
        collection.insertOne(json("_id: 3, b: {value: 1}"));
        collection.insertOne(json("_id: 4, a: {value: 5}"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("_id: null, n: 4, sumOfA: 45, sumOfB: 31.5"));
    }

    @Test
    public void testAggregateWithGroupByMinAndMax() throws Exception {
        Document query = json("_id: null, minA: {$min: '$a'}, maxB: {$max: '$b.value'}, maxC: {$max: '$c'}, minC: {$min: '$c'}");
        List<Document> pipeline = Collections.singletonList(new Document("$group", query));

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, a: 30, b: {value: 20}, c: 1.0"));
        collection.insertOne(json("_id: 2, a: 15, b: {value: 10}, c: 2"));
        collection.insertOne(json("_id: 3, c: 'zzz'"));
        collection.insertOne(json("_id: 4, c: 'aaa'"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("_id: null, minA: 15, maxB: 20, minC: 1.0, maxC: 'zzz'"));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/68
    @Test
    public void testAggregateWithGroupByMinAndMaxOnArrayField() throws Exception {
        Document query = json("_id: null, min: {$min: '$v'}, max: {$max: '$v'}");
        List<Document> pipeline = Collections.singletonList(new Document("$group", query));

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, v: [10, 20, 30]"));
        collection.insertOne(json("_id: 2, v: [3, 40]"));
        collection.insertOne(json("_id: 3, v: [11, 25]"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("_id: null, max: [11, 25], min: [3, 40]"));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/68
    @Test
    public void testAggregateWithGroupByMinAndMaxOnArrayFieldAndNonArrayFields() throws Exception {
        Document query = json("_id: null, min: {$min: '$v'}, max: {$max: '$v'}");
        List<Document> pipeline = Collections.singletonList(new Document("$group", query));

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, v: [10, 20, 30]"));
        collection.insertOne(json("_id: 2, v: [3, 40]"));
        collection.insertOne(json("_id: 3, v: [11, 25]"));
        collection.insertOne(json("_id: 4, v: 50"));
        collection.insertOne(json("_id: 5, v: null"));
        collection.insertOne(json("_id: 6"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("_id: null, max: [11, 25], min: 50"));
    }

    @Test
    public void testAggregateWithGroupByNonExistingMinAndMax() throws Exception {
        Document query = json("_id: null, minOfA: {$min: '$doesNotExist'}, maxOfB: {$max: '$doesNotExist'}");
        List<Document> pipeline = Collections.singletonList(new Document("$group", query));

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, a: 30, b: {value: 20}"));
        collection.insertOne(json("_id: 2, a: 15, b: {value: 10}"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("_id: null, minOfA: null, maxOfB: null"));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/104#issuecomment-548151945
    @Test
    public void testMinMaxAvgProjectionOfArrayValues() throws Exception {
        List<Document> pipeline = jsonList("$project: {min: {$min: '$v'}, max: {$max: '$v'}, avg: {$avg: '$v'}}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, v: [10, 20, 30]"));
        collection.insertOne(json("_id: 2, v: [3, 40]"));
        collection.insertOne(json("_id: 3, v: [11, 25]"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("_id: 1, min: 10, max: 30, avg: 20.0"),
                json("_id: 2, min: 3, max: 40, avg: 21.5"),
                json("_id: 3, min: 11, max: 25, avg: 18.0"));
    }

    @Test
    public void testMinMaxAvgProjectionOfNonArrayValue() throws Exception {
        List<Document> pipeline = jsonList("$project: {min: {$min: '$v'}, max: {$max: '$v'}, avg: {$avg: '$v'}}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, v: 'abc'"));
        collection.insertOne(json("_id: 2"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("_id: 1, min: 'abc', max: 'abc', avg: null"),
                json("_id: 2, min: null, max: null, avg: null")
            );
    }

    @Test
    public void testMinMaxAvgProjectionWithTwoParameters() throws Exception {
        List<Document> pipeline = jsonList("$project: {min: {$min: ['$v', 2]}, max: {$max: ['$v', 2]}, avg: {$avg: ['$v', 2]}}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, v: 10"));
        collection.insertOne(json("_id: 2, v: 0"));
        collection.insertOne(json("_id: 3"));
        collection.insertOne(json("_id: 4, v: 'abc'"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("_id: 1, min: 2, max: 10, avg: 6.0"),
                json("_id: 2, min: 0, max: 2, avg: 1.0"),
                json("_id: 3, min: 2, max: 2, avg: 2.0"),
                json("_id: 4, min: 2, max: 'abc', avg: 2.0")
            );
    }

    @Test
    public void testMinMaxAvgProjectionWithOneParameter() throws Exception {
        List<Document> pipeline = jsonList("$project: {min: {$min: 'abc'}, max: {$max: 'def'}, avg: {$avg: 10}}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("_id: 1, min: 'abc', max: 'def', avg: 10.0"));
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
        assertThat(collection.aggregate(Collections.emptyList())).isEmpty();

        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));

        assertThat(collection.aggregate(Collections.emptyList()))
            .containsExactly(json("_id: 1"), json("_id: 2"));
    }

    @Test
    public void testAggregateWithMissingIdInGroupSpecification() throws Exception {
        List<Document> pipeline = Collections.singletonList(new Document("$group", json("n: {$sum: 1}")));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("Command failed with error 15955 (Location15955): 'a group specification must include an _id'");
    }

    @Test
    public void testAggregateWithGroupBySumPipeline() throws Exception {
        Document query = json("_id: null, n: {$sum: 1}");
        List<Document> pipeline = Collections.singletonList(new Document("$group", query));

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("_id: null, n: 2"));

        query.putAll(json("n: {$sum: 'abc'}"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("_id: null, n: 0"));

        query.putAll(json("n: {$sum: 2}"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("_id: null, n: 4"));

        query.putAll(json("n: {$sum: 1.75}"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("_id: null, n: 3.5"));

        query.putAll(new Document("n", new Document("$sum", 10000000000L)));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("_id: null, n: 20000000000"));

        query.putAll(new Document("n", new Document("$sum", -2.5F)));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("_id: null, n: -5.0"));
    }

    @Test
    public void testAggregateWithGroupByAvg() throws Exception {
        Document query = json("_id: null, avg: {$avg: 1}");
        List<Document> pipeline = Collections.singletonList(new Document("$group", query));

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, a: 6.0, b: 'zzz'"));
        collection.insertOne(json("_id: 2, a: 3.0, b: 'aaa'"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("_id: null, avg: 1.0"));

        query.putAll(json("avg: {$avg: '$a'}, avgB: {$avg: '$b'}"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("_id: null, avg: 4.5, avgB: null"));
    }

    @Test
    public void testAggregateWithGroupByKey() throws Exception {
        List<Document> pipeline = jsonList("$group: {_id: '$a', count: {$sum: 1}, avg: {$avg: '$b'}}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, a: 1"));
        collection.insertOne(json("_id: 2, a: 1"));
        collection.insertOne(json("_id: 3, a: 2, b: 3"));
        collection.insertOne(json("_id: 4, a: 2, b: 4"));
        collection.insertOne(json("_id: 5, a: 5, b: 10"));
        collection.insertOne(json("_id: 6, a: 7, c: 'a'"));
        collection.insertOne(json("_id: 7"));

        assertThat(collection.aggregate(pipeline))
            .containsExactlyInAnyOrder(
                json("_id: 1, count: 2, avg: null"),
                json("_id: 2, count: 2, avg: 3.5"),
                json("_id: 5, count: 1, avg: 10.0"),
                json("_id: 7, count: 1, avg: null"),
                json("_id: null, count: 1, avg: null")
            );
    }

    @Test
    public void testAggregateWithGroupByNumberEdgeCases() throws Exception {
        String groupBy = "$group: {_id: '$a', count: {$sum: 1}, avg: {$avg: '$a'}, min: {$min: '$a'}, max: {$max: '$a'}}";
        List<Document> pipeline = jsonList(groupBy);

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, a: 1.0"));
        collection.insertOne(json("_id: 2, a: 1"));
        collection.insertOne(json("_id: 3, a: -0.0"));
        collection.insertOne(json("_id: 4, a: 0.0"));
        collection.insertOne(json("_id: 5, a: 0"));
        collection.insertOne(json("_id: 6, a: 1.5"));
        collection.insertOne(json("_id: 7, a: null"));

        assertThat(collection.aggregate(pipeline))
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

        assertThat(collection.aggregate(pipeline)).isEmpty();

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

        assertThat(collection.aggregate(pipeline))
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
    public void testAggregateWithGroupByIllegalKey() throws Exception {
        collection.insertOne(json("_id:  1, a: 1"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$group: {_id: '$a.'}")).first())
            .withMessageContaining("Command failed with error 40353 (Location40353): 'FieldPath must not end with a '.'.'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$group: {_id: '$a..1'}")).first())
            .withMessageContaining("Command failed with error 15998 (Location15998): 'FieldPath field names may not be empty strings.'");
    }

    @Test
    public void testAggregateWithSimpleExpressions() throws Exception {
        List<Document> pipeline = jsonList("$group: {_id: {$abs: '$value'}, count: {$sum: 1}}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, value: 1"));
        collection.insertOne(json("_id: -2, value: -1"));
        collection.insertOne(json("_id: 3, value: 2"));
        collection.insertOne(json("_id: 4, value: 2"));
        collection.insertOne(json("_id: 5, value: -2.5"));

        assertThat(collection.aggregate(pipeline))
            .containsExactlyInAnyOrder(
                json("_id: 1, count: 2"),
                json("_id: 2, count: 2"),
                json("_id: 2.5, count: 1")
            );
    }

    @Test
    public void testAggregateWithMultipleExpressionsInKey() throws Exception {
        List<Document> pipeline = jsonList("$group: {_id: {abs: {$abs: '$value'}, sum: {$subtract: ['$end', '$start']}}, count: {$sum: 1}}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, value: NaN"));
        collection.insertOne(json("_id: 2, value: 1, start: 5, end: 8"));
        collection.insertOne(json("_id: 3, value: -1, start: 4, end: 4"));
        collection.insertOne(json("_id: 4, value: 2, start: 9, end: 7"));
        collection.insertOne(json("_id: 5, value: 2, start: 6, end: 7"));

        assertThat(collection.aggregate(pipeline))
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
        List<Document> pipeline = jsonList(
            "$group: {_id: { day: { $dayOfYear: '$date'}, year: { $year: '$date' } }, itemsSold: { $addToSet: '$item' }}",
            "$sort: {_id: 1}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, item: 'zzz', price:  5, quantity: 10").append("date", instant("2014-02-15T09:12:00Z")));
        collection.insertOne(json("_id: 2, item: 'abc', price: 10, quantity:  2").append("date", instant("2014-01-01T08:00:00Z")));
        collection.insertOne(json("_id: 3, item: 'jkl', price: 20, quantity:  1").append("date", instant("2014-02-03T09:00:00Z")));
        collection.insertOne(json("_id: 4, item: 'xyz', price:  5, quantity:  5").append("date", instant("2014-02-03T09:05:00Z")));
        collection.insertOne(json("_id: 5, item: 'abc', price: 10, quantity: 10").append("date", instant("2014-02-15T08:00:00Z")));
        collection.insertOne(json("_id: 6, item: 'xyz', price:  5, quantity: 10").append("date", instant("2014-02-15T09:12:00Z")));

        assertThat(collection.aggregate(pipeline).map(withSortedStringList("itemsSold")))
            .containsExactlyInAnyOrder(
                json("_id: { day:  1, year: 2014 }, itemsSold: [ 'abc' ]"),
                json("_id: { day: 34, year: 2014 }, itemsSold: [ 'jkl', 'xyz' ]"),
                json("_id: { day: 46, year: 2014 }, itemsSold: [ 'abc', 'xyz', 'zzz' ]")
            );
    }

    @Test
    public void testAggregateWithEmptyAddToSet() throws Exception {
        List<Document> pipeline = jsonList("$group: {_id: 1, set: { $addToSet: '$foo' }}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("_id: 1, set: [ ]"));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/111
    @Test
    public void testAggregateWithAddToSetAndMissingValue() throws Exception {
        List<Document> pipeline = jsonList("$group: {\n" +
            "    _id: '$_id', \n" +
            "    sources: {\n" +
            "        $addToSet: {\n" +
            "            key: '$content.key',\n" +
            "            missing: '$content.missing'\n" +
            "        }\n" +
            "    }\n" +
            "}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, content: {key: 'value', key2: 'value2'}"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("_id: 1, sources: [{key: 'value'}]"));
    }

    @Test
    public void testAggregateWithAdd() throws Exception {
        List<Document> pipeline = jsonList("$project: { item: 1, total: { $add: [ '$price', '$fee' ] } }");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, item: 'abc', price: 10, fee: 2"));
        collection.insertOne(json("_id: 2, item: 'jkl', price: 20, fee: 1"));
        collection.insertOne(json("_id: 3, item: 'xyz', price: 5, fee: 0"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("_id: 1, item: 'abc', total: 12"),
                json("_id: 2, item: 'jkl', total: 21"),
                json("_id: 3, item: 'xyz', total: 5 ")
            );
    }

    @Test
    public void testAggregateWithSort() throws Exception {
        List<Document> pipeline = jsonList("$sort: { price: -1, fee: 1 }");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, price: 10, fee: 1"));
        collection.insertOne(json("_id: 2, price: 20, fee: 0"));
        collection.insertOne(json("_id: 3, price: 10, fee: 0"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("_id: 2, price: 20, fee: 0"),
                json("_id: 3, price: 10, fee: 0"),
                json("_id: 1, price: 10, fee: 1")
            );
    }

    @Test
    public void testAggregateWithProjection() throws Exception {
        List<Document> pipeline = jsonList("$project: {_id: 1, value: '$x', n: '$foo.bar', other: null}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, x: 10, foo: 'abc'"));
        collection.insertOne(json("_id: 2, x: 20"));
        collection.insertOne(json("_id: 3, x: 30, foo: {bar: 7.3}"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("_id: 1, value: 10, other: null"),
                json("_id: 2, value: 20, other: null"),
                json("_id: 3, value: 30, n: 7.3, other: null")
            );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/121
    @Test
    public void testAggregateWithNestedExclusiveProjection() throws Exception {
        List<Document> pipeline = jsonList("$project: {_id: 0, 'x.b': 0}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, x: {a: 1, b: 2, c: 3}"));
        collection.insertOne(json("_id: 2, x: 20"));
        collection.insertOne(json("_id: 3"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("x: {a: 1, c: 3}"),
                json("x: 20"),
                json("")
            );
    }

    @Test
    public void testAggregateWithNestedInclusiveProjection() throws Exception {
        List<Document> pipeline = jsonList("$project: {'x.b': 1, 'x.c': 1}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, x: {b: 2, c: 3}"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("_id: 1, x: {b: 2, c: 3}"),
                json("_id: 2"),
                json("_id: 3")
            );
    }

    @Test
    public void testAggregateWithIllegalProjection() throws Exception {
        List<Document> pipeline = jsonList("$project: {'x.b': 1, 'x.c': 1, 'x.d': 0, y: 0}");

        collection.insertOne(json("_id: 1"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("Command failed with error 40178 (Location40178): " +
                "'Bad projection specification, cannot exclude fields other than '_id' in an inclusion projection: { x.b: 1, x.c: 1, x.d: 0, y: 0 }'");
    }

    @Test
    public void testAggregateWithProjection_IllegalFieldPath() throws Exception {
        collection.insertOne(json("_id: 1, x: 10"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {_id: 0, v: '$x.1.'}")).first())
            .withMessageContaining("Command failed with error 40353 (Location40353): 'FieldPath must not end with a '.'.'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {_id: 0, v: '$x..1'}")).first())
            .withMessageContaining("Command failed with error 15998 (Location15998): 'FieldPath field names may not be empty strings.'");
    }

    @Test
    public void testAggregateWithExpressionProjection() throws Exception {
        List<Document> pipeline = jsonList("$project: {_id: 0, idHex: {$toString: '$_id'}}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(new Document("_id", new ObjectId("abcd01234567890123456789")));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("idHex: 'abcd01234567890123456789'"));
    }

    @Test
    public void testAggregateWithStrLenExpressionProjection() throws Exception {
        List<Document> pipeline = jsonList("$project: {_id: 0, lenCP: {$strLenCP: '$a'}, lenBytes: {$strLenBytes: '$a'}}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("a: 'cafétéria', b: 123"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("lenCP: 9, lenBytes: 11"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {len: {$strLenCP: '$x'}}")).first())
            .withMessageContaining("Command failed with error 34471 (Location34471): '$strLenCP requires a string argument, found: missing'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {len: {$strLenCP: '$b'}}")).first())
            .withMessageContaining("Command failed with error 34471 (Location34471): '$strLenCP requires a string argument, found: int'");
    }

    @Test
    public void testAggregateWithSubstringExpressionProjection() throws Exception {
        List<Document> pipeline = jsonList("$project: {_id: 0, " +
            "a: {$substr: ['$v', 0, -1]}, " +
            "b: {$substr: ['$v', 1, -3]}, " +
            "c: {$substr: ['$v', 5, 5]}, " +
            "d: {$substr: [123, 0, -1]}, " +
            "e: {$substr: [null, 0, -1]}" +
            "f: {$substr: ['abc', 4, -1]}" +
            "}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("v: 'some value'"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("a: 'some value', b: 'ome value', c: 'value', d: '123', e: '', f: ''"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {x: {$substr: 'abc'}}")).first())
            .withMessageContaining("Command failed with error 16020 (Location16020): 'Expression $substrBytes takes exactly 3 arguments. 1 were passed in.'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {x: {$substr: ['abc', 'abc', 3]}}")).first())
            .withMessageContaining("Command failed with error 16034 (Location16034): 'Failed to optimize pipeline :: caused by :: $substrBytes:  starting index must be a numeric type (is BSON type string)");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {x: {$substr: ['abc', 3, 'abc']}}")).first())
            .withMessageContaining("Command failed with error 16035 (Location16035): 'Failed to optimize pipeline :: caused by :: $substrBytes:  length must be a numeric type (is BSON type string)");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {x: {$substrCP: 'abc'}}")).first())
            .withMessageContaining("Command failed with error 16020 (Location16020): 'Expression $substrCP takes exactly 3 arguments. 1 were passed in.'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {x: {$substrCP: ['abc', 'abc', 3]}}")).first())
            .withMessageContaining("Command failed with error 34450 (Location34450): 'Failed to optimize pipeline :: caused by :: $substrCP: starting index must be a numeric type (is BSON type string)");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {x: {$substrCP: ['abc', 3, 'abc']}}")).first())
            .withMessageContaining("Command failed with error 34452 (Location34452): 'Failed to optimize pipeline :: caused by :: $substrCP: length must be a numeric type (is BSON type string)");
    }

    @Test
    public void testAggregateWithSubstringUnicodeExpressionProjection() throws Exception {
        List<Document> pipeline = jsonList("$project: {_id: 0, " +
            "a: {$substrBytes: ['$v', 0, 5]}, " +
            "b: {$substrCP: ['$v', 0, 5]}, " +
            "c: {$substrBytes: ['$v', 5, 4]}, " +
            "d: {$substrCP: ['$v', 5, 4]}, " +
            "}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("v: 'cafétéria'"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("a: 'café', b: 'cafét', c: 'tér', d: 'éria'"));
    }

    @Test
    public void testAggregateWithAddFields() throws Exception {
        List<Document> pipeline = jsonList("$addFields: {value: '$x'}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, x: 10"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3, value: 123"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("_id: 1, x: 10, value: 10"),
                json("_id: 2"),
                json("_id: 3")
            );
    }

    @Test
    public void testAggregateWithMultipleMatches() throws Exception {
        List<Document> pipeline = jsonList(
            "$match: {price: {$lt: 100}}",
            "$match: {quality: {$gt: 10}}"
        );

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, price: 10, quality: 50"));
        collection.insertOne(json("_id: 2, price: 150, quality: 500"));
        collection.insertOne(json("_id: 3, price: 50, quality: 150"));
        collection.insertOne(json("_id: 4, price: 10, quality: 5"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("_id: 1, price: 10, quality: 50"),
                json("_id: 3, price: 50, quality: 150")
            );
    }

    @Test
    public void testAggregateWithLogicalAndInMatch() throws Exception {
        List<Document> pipeline = jsonList("$match: {$and: [{price: {$lt: 100}}, {quality: {$gt: 10}}]}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, price: 10, quality: 50"));
        collection.insertOne(json("_id: 2, price: 150, quality: 500"));
        collection.insertOne(json("_id: 3, price: 50, quality: 150"));
        collection.insertOne(json("_id: 4, price: 10, quality: 5"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("_id: 1, price: 10, quality: 50"),
                json("_id: 3, price: 50, quality: 150")
            );
    }

    @Test
    public void testAggregateWithLogicalAndInMatchExpr() throws Exception {
        List<Document> pipeline = jsonList("$match: {$expr: {$and: [{$lt: ['$price', 100]}, {$gt: ['$quality', 10]}]}}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, price: 10, quality: 50"));
        collection.insertOne(json("_id: 2, price: 150, quality: 500"));
        collection.insertOne(json("_id: 3, price: 50, quality: 150"));
        collection.insertOne(json("_id: 4, price: 10, quality: 5"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("_id: 1, price: 10, quality: 50"),
                json("_id: 3, price: 50, quality: 150")
            );
    }

    @Test
    public void testAggregateWithLogicalOrInMatchExpr() throws Exception {
        List<Document> pipeline = jsonList("$match: {$expr: {$or: [{$gt: ['$price', 100]}, {$gt: ['$quality', 70]}]}}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, price: 10, quality: 50"));
        collection.insertOne(json("_id: 2, price: 150, quality: 500"));
        collection.insertOne(json("_id: 3, price: 50, quality: 150"));
        collection.insertOne(json("_id: 4, price: 10, quality: 5"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("_id: 2, price: 150, quality: 500"),
                json("_id: 3, price: 50, quality: 150")
            );
    }

    @Test
    public void testAggregateWithLogicalOrInMatch() throws Exception {
        List<Document> pipeline = jsonList("$match: {$or: [{$and: [{price: {$lt: 50}}, {quality: {$gt: 10}}]}, {quality: {$gt: 200}}]}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, price: 10, quality: 50"));
        collection.insertOne(json("_id: 2, price: 150, quality: 500"));
        collection.insertOne(json("_id: 3, price: 50, quality: 150"));
        collection.insertOne(json("_id: 4, price: 10, quality: 5"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("_id: 1, price: 10, quality: 50"),
                json("_id: 2, price: 150, quality: 500")
            );
    }

    @Test
    public void testAggregateWithCeil() throws Exception {
        List<Document> pipeline = jsonList("$project: {a: 1, ceil: {$ceil: '$a'}}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, a: 9.25"));
        collection.insertOne(json("_id: 2, a: 8.73"));
        collection.insertOne(json("_id: 3, a: 4.32"));
        collection.insertOne(json("_id: 4, a: -5.34"));

        assertThat(collection.aggregate(pipeline))
            .containsExactlyInAnyOrder(
                json("_id: 1, a: 9.25, ceil: 10.0"),
                json("_id: 2, a: 8.73, ceil: 9.0"),
                json("_id: 3, a: 4.32, ceil: 5.0"),
                json("_id: 4, a: -5.34, ceil: -5.0")
            );
    }

    @Test
    public void testAggregateWithNumericOperators() throws Exception {
        List<Document> pipeline = jsonList("$project: {a: 1, exp: {$exp: '$a'}, ln: {$ln: '$a'}, log10: {$log10: '$a'}, sqrt: {$sqrt: '$a'}}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, a: 1"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("_id: 1, a: 1, exp: 2.718281828459045, ln: 0.0, log10: 0.0, sqrt: 1.0"));
    }

    @Test
    public void testAggregateWithCount() throws Exception {
        Document match = json("$match: {score: {$gt: 80}}");
        Document count = json("$count: 'passing_scores'");
        List<Document> pipeline = Arrays.asList(match, count);

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, subject: 'History', score: 88"));
        collection.insertOne(json("_id: 2, subject: 'History', score: 92"));
        collection.insertOne(json("_id: 3, subject: 'History', score: 97"));
        collection.insertOne(json("_id: 4, subject: 'History', score: 71"));
        collection.insertOne(json("_id: 5, subject: 'History', score: 79"));
        collection.insertOne(json("_id: 6, subject: 'History', score: 83"));

        assertThat(collection.aggregate(pipeline)).containsExactly(json("passing_scores: 4"));
    }

    @Test
    public void testAggregateWithFirstAndLast() throws Exception {
        Document sort = json("$sort: { item: 1, date: 1 }");
        Document group = json("$group: {_id: '$item', firstSale: { $first: '$date' }, lastSale: { $last: '$date'} }");
        List<Document> pipeline = Arrays.asList(sort, group);

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, item: 'abc', price: 10, quantity:  2").append("date", instant("2014-01-01T08:00:00Z")));
        collection.insertOne(json("_id: 2, item: 'jkl', price: 20, quantity:  1").append("date", instant("2014-02-03T09:00:00Z")));
        collection.insertOne(json("_id: 3, item: 'xyz', price:  5, quantity:  5").append("date", instant("2014-02-03T09:05:00Z")));
        collection.insertOne(json("_id: 4, item: 'abc', price: 10, quantity: 10").append("date", instant("2014-02-15T08:00:00Z")));
        collection.insertOne(json("_id: 5, item: 'xyz', price:  5, quantity: 10").append("date", instant("2014-02-15T09:12:00Z")));

        assertThat(collection.aggregate(pipeline))
            .containsExactlyInAnyOrder(
                json("_id: 'abc'").append("firstSale", date("2014-01-01T08:00:00Z")).append("lastSale", date("2014-02-15T08:00:00Z")),
                json("_id: 'jkl'").append("firstSale", date("2014-02-03T09:00:00Z")).append("lastSale", date("2014-02-03T09:00:00Z")),
                json("_id: 'xyz'").append("firstSale", date("2014-02-03T09:05:00Z")).append("lastSale", date("2014-02-15T09:12:00Z"))
            );
    }

    @Test
    public void testAggregateWithPush() throws Exception {
        List<Document> pipeline = jsonList("$group: {_id: null, a: {$push: '$a'}, b: {$push: {v: '$b'}}, c: {$push: '$c'}}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, a: 10, b: 0.1"));
        collection.insertOne(json("_id: 2, a: 20, b: 0.2"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("_id: null, a: [10, 20], b: [{v: 0.1}, {v: 0.2}], c: []"));
    }

    @Test
    public void testAggregateWithUndefinedVariable() throws Exception {
        List<Document> pipeline = jsonList("$project: {result: '$$UNDEFINED'}");

        collection.insertOne(json("_id: 1"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("Command failed with error 17276 (Location17276): 'Use of undefined variable: UNDEFINED'");
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/31
    @Test
    public void testAggregateWithRootVariable() throws Exception {
        List<Document> pipeline = jsonList("$project: {_id: 0, doc: '$$ROOT', a: '$$ROOT.a', a_v: '$$ROOT.a.v'}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, a: {v: 10}"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("doc: {_id: 1, a: {v: 10}}, a: {v: 10}, a_v: 10"));
    }

    @Test
    public void testAggregateWithRootVariable_IllegalFieldPath() throws Exception {
        collection.insertOne(json("_id: 1, x: 10"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {_id: '$$ROOT.a.'}")).first())
            .withMessageContaining("Command failed with error 40353 (Location40353): 'FieldPath must not end with a '.'.'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {_id: '$$ROOT.a..1'}")).first())
            .withMessageContaining("Command failed with error 15998 (Location15998): 'FieldPath field names may not be empty strings.'");
    }

    @Test
    public void testAggregateWithSetOperations() throws Exception {
        List<Document> pipeline = jsonList("$project: {union: {$setUnion: ['$a', '$b']}, diff: {$setDifference: ['$a', '$b']}, intersection: {$setIntersection: ['$a', '$b']}}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, a: [3, 2, 1]"));
        collection.insertOne(json("_id: 2, a: [1.0, -0.0], b: [3, 2, 0]"));
        collection.insertOne(json("_id: 3, a: [{a: 0}, {a: 1}], b: [{a: 0.0}, {a: 0.5}]"));

        assertThat(collection.aggregate(pipeline))
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

        List<Document> pipeline = jsonList("$project: {cmp1: {$cmp: ['$v', 10]}, cmp2: {$cmp: ['$v', [10]]}}");
        Document project;

        assertThat(collection.aggregate(pipeline))
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

        assertThat(collection.aggregate(pipeline))
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

        assertThat(collection.aggregate(pipeline))
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
    public void testAggregateWithSlice() throws Exception {
        List<Document> pipeline = jsonList("$project: {name: 1, threeFavorites: {$slice: ['$favorites', 3]}}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, name: 'dave123', favorites: ['chocolate', 'cake', 'butter', 'apples']"));
        collection.insertOne(json("_id: 2, name: 'li', favorites: ['apples', 'pudding', 'pie']"));
        collection.insertOne(json("_id: 3, name: 'ahn', favorites: ['pears', 'pecans', 'chocolate', 'cherries']"));
        collection.insertOne(json("_id: 4, name: 'ty', favorites: ['ice cream']"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("_id: 1, name: 'dave123', threeFavorites: ['chocolate', 'cake', 'butter']"),
                json("_id: 2, name: 'li', threeFavorites: ['apples', 'pudding', 'pie']"),
                json("_id: 3, name: 'ahn', threeFavorites: ['pears', 'pecans', 'chocolate']"),
                json("_id: 4, name: 'ty', threeFavorites: ['ice cream']")
            );
    }

    @Test
    public void testAggregateWithSplit() throws Exception {
        List<Document> pipeline = jsonList("$project: {_id: 1, names: {$split: ['$name', ' ']}}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, name: 'first document'"));
        collection.insertOne(json("_id: 2, name: 'second document'"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("_id: 1, names: ['first', 'document']"),
                json("_id: 2, names: ['second', 'document']")
            );
    }

    @Test
    public void testAggregateWithUnwind() throws Exception {
        testAggregateWithUnwind(json("$unwind: '$sizes'"));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/54
    @Test
    public void testAggregateWithUnwind_Path() throws Exception {
        testAggregateWithUnwind(json("$unwind: {path: '$sizes'}"));
    }

    private void testAggregateWithUnwind(Document unwind) throws Exception {
        List<Document> pipeline = Collections.singletonList(unwind);

        assertThat(collection.aggregate(pipeline)).isEmpty();
        collection.insertOne(json("_id: 1, item: 'ABC', sizes: ['S', 'M', 'L']"));
        collection.insertOne(json("_id: 2, item: 'EFG', sizes: []"));
        collection.insertOne(json("_id: 3, item: 'IJK', sizes: 'M'"));
        collection.insertOne(json("_id: 4, item: 'LMN'"));
        collection.insertOne(json("_id: 5, item: 'XYZ', sizes: null"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("_id: 1, item: 'ABC', sizes: 'S'"),
                json("_id: 1, item: 'ABC', sizes: 'M'"),
                json("_id: 1, item: 'ABC', sizes: 'L'"),
                json("_id: 3, item: 'IJK', sizes: 'M'")
            );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/54
    @Test
    public void testAggregateWithUnwind_preserveNullAndEmptyArrays() throws Exception {
        List<Document> pipeline = jsonList("$unwind: {path: '$sizes', preserveNullAndEmptyArrays: true}");

        assertThat(collection.aggregate(pipeline)).isEmpty();
        collection.insertOne(json("_id: 1, item: 'ABC', sizes: ['S', 'M', 'L']"));
        collection.insertOne(json("_id: 2, item: 'EFG', sizes: []"));
        collection.insertOne(json("_id: 3, item: 'IJK', sizes: 'M'"));
        collection.insertOne(json("_id: 4, item: 'LMN'"));
        collection.insertOne(json("_id: 5, item: 'XYZ', sizes: null"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("_id: 1, item: 'ABC', sizes: 'S'"),
                json("_id: 1, item: 'ABC', sizes: 'M'"),
                json("_id: 1, item: 'ABC', sizes: 'L'"),
                json("_id: 2, item: 'EFG'"),
                json("_id: 3, item: 'IJK', sizes: 'M'"),
                json("_id: 4, item: 'LMN'"),
                json("_id: 5, item: 'XYZ', sizes: null")
            );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/54
    @Test
    public void testAggregateWithUnwind_IncludeArrayIndex() throws Exception {
        List<Document> pipeline = jsonList("$unwind: {path: '$sizes', includeArrayIndex: 'idx'}");

        assertThat(collection.aggregate(pipeline)).isEmpty();
        collection.insertOne(json("_id: 1, item: 'ABC', sizes: ['S', 'M', 'L']"));
        collection.insertOne(json("_id: 2, item: 'EFG', sizes: []"));
        collection.insertOne(json("_id: 3, item: 'IJK', sizes: 'M'"));
        collection.insertOne(json("_id: 4, item: 'LMN'"));
        collection.insertOne(json("_id: 5, item: 'XYZ', sizes: null"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("_id: 1, item: 'ABC', sizes: 'S'").append("idx", 0L),
                json("_id: 1, item: 'ABC', sizes: 'M'").append("idx", 1L),
                json("_id: 1, item: 'ABC', sizes: 'L'").append("idx", 2L),
                json("_id: 3, item: 'IJK', sizes: 'M'").append("idx", null)
            );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/54
    @Test
    public void testAggregateWithUnwind_IncludeArrayIndex_OverwriteExistingField() throws Exception {
        List<Document> pipeline = jsonList("$unwind: {path: '$sizes', includeArrayIndex: 'item'}");

        assertThat(collection.aggregate(pipeline)).isEmpty();
        collection.insertOne(json("_id: 1, item: 'ABC', sizes: ['S', 'M', 'L']"));
        collection.insertOne(json("_id: 2, item: 'EFG', sizes: []"));
        collection.insertOne(json("_id: 3, item: 'IJK', sizes: 'M'"));
        collection.insertOne(json("_id: 4, item: 'LMN'"));
        collection.insertOne(json("_id: 5, item: 'XYZ', sizes: null"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("_id: 1, sizes: 'S'").append("item", 0L),
                json("_id: 1, sizes: 'M'").append("item", 1L),
                json("_id: 1, sizes: 'L'").append("item", 2L),
                json("_id: 3, sizes: 'M'").append("item", null)
            );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/54
    @Test
    public void testAggregateWithUnwind_IncludeArrayIndex_NestedIndexField() throws Exception {
        List<Document> pipeline = jsonList("$unwind: {path: '$sizes', includeArrayIndex: 'item.idx'}");

        assertThat(collection.aggregate(pipeline)).isEmpty();
        collection.insertOne(json("_id: 1, item: {value: 'ABC'}, sizes: ['S', 'M', 'L']"));
        collection.insertOne(json("_id: 2, item: {value: 'EFG'}, sizes: []"));
        collection.insertOne(json("_id: 3, item: {value: 'IJK'}, sizes: 'M'"));
        collection.insertOne(json("_id: 4, item: {value: 'LMN'}"));
        collection.insertOne(json("_id: 5, item: {value: 'XYZ'}, sizes: null"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("_id: 1, sizes: 'S'").append("item", json("value: 'ABC'").append("idx", 0L)),
                json("_id: 1, sizes: 'M'").append("item", json("value: 'ABC'").append("idx", 1L)),
                json("_id: 1, sizes: 'L'").append("item", json("value: 'ABC'").append("idx", 2L)),
                json("_id: 3, sizes: 'M'").append("item", json("value: 'IJK'").append("idx", null))
            );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/54
    @Test
    public void testAggregateWithUnwind_preserveNullAndEmptyArraysAndIncludeArrayIndex() throws Exception {
        List<Document> pipeline = jsonList("$unwind: {path: '$sizes', preserveNullAndEmptyArrays: true, includeArrayIndex: 'idx'}");

        assertThat(collection.aggregate(pipeline)).isEmpty();
        collection.insertOne(json("_id: 1, item: 'ABC', sizes: ['S', 'M', 'L']"));
        collection.insertOne(json("_id: 2, item: 'EFG', sizes: []"));
        collection.insertOne(json("_id: 3, item: 'IJK', sizes: 'M'"));
        collection.insertOne(json("_id: 4, item: 'LMN'"));
        collection.insertOne(json("_id: 5, item: 'XYZ', sizes: null"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("_id: 1, item: 'ABC', sizes: 'S'").append("idx", 0L),
                json("_id: 1, item: 'ABC', sizes: 'M'").append("idx", 1L),
                json("_id: 1, item: 'ABC', sizes: 'L'").append("idx", 2L),
                json("_id: 2, item: 'EFG'").append("idx", null),
                json("_id: 3, item: 'IJK', sizes: 'M'").append("idx", null),
                json("_id: 4, item: 'LMN'").append("idx", null),
                json("_id: 5, item: 'XYZ', sizes: null").append("idx", null)
            );
    }

    @Test
    public void testAggregateWithUnwind_subdocumentArray() throws Exception {
        List<Document> pipeline = Collections.singletonList(json("$unwind: {path: '$items.sizes'}"));

        assertThat(collection.aggregate(pipeline)).isEmpty();
        collection.insertOne(json("_id: 1, items: [{sizes: ['S', 'M', 'L']}]"));
        collection.insertOne(json("_id: 2, items: [{sizes: 'M'}]"));
        collection.insertOne(json("_id: 3, items: {sizes: ['XL', 'S']}"));
        collection.insertOne(json("_id: 4, items: [{sizes: ['M', 'L']}]"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("_id: 3, items: {sizes: 'XL'}"),
                json("_id: 3, items: {sizes: 'S'}")
            );
    }

    @Test
    public void testAggregateWithLookup() {
        MongoCollection<Document> authorsCollection = db.getCollection("authors");
        authorsCollection.insertOne(json("_id: 1, name: 'Uncle Bob'"));
        authorsCollection.insertOne(json("_id: 2, name: 'Martin Fowler'"));
        authorsCollection.insertOne(json("_id: null, name: 'Null Author'"));

        List<Document> pipeline = jsonList("$lookup: {from: 'authors', localField: 'authorId', foreignField: '_id', as: 'author'}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, title: 'Refactoring', authorId: 2"));
        collection.insertOne(json("_id: 2, title: 'Clean Code', authorId: 1"));
        collection.insertOne(json("_id: 3, title: 'Clean Coder', authorId: 1"));
        collection.insertOne(json("_id: 4, title: 'Unknown author', authorId: 3"));
        collection.insertOne(json("_id: 5, title: 'No author', authorId: null"));
        collection.insertOne(json("_id: 6, title: 'Missing author'"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("_id: 1, title: 'Refactoring', authorId: 2, author: [{_id: 2, name: 'Martin Fowler'}]"),
                json("_id: 2, title: 'Clean Code', authorId: 1, author: [{_id: 1, name: 'Uncle Bob'}]"),
                json("_id: 3, title: 'Clean Coder', authorId: 1, author: [{_id: 1, name: 'Uncle Bob'}]"),
                json("_id: 4, title: 'Unknown author', authorId: 3, author: []"),
                json("_id: 5, title: 'No author', authorId: null, author: [{_id: null, name: 'Null Author'}]"),
                json("_id: 6, title: 'Missing author', author: [{_id: null, name: 'Null Author'}]")
            );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/123
    @Test
    public void testAggregateWithLookup_collectionDoesNotExist() {
        List<Document> pipeline = jsonList("$lookup: {from: 'authors', localField: 'authorId', foreignField: '_id', as: 'author'}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, title: 'Refactoring', authorId: 2"));
        collection.insertOne(json("_id: 2, title: 'Clean Code', authorId: 1"));
        collection.insertOne(json("_id: 3, title: 'Clean Coder', authorId: 1"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("_id: 1, title: 'Refactoring', authorId: 2, author: []"),
                json("_id: 2, title: 'Clean Code', authorId: 1, author: []"),
                json("_id: 3, title: 'Clean Coder', authorId: 1, author: []")
            );
    }

    @Test
    public void testAggregateWithIllegalLookupStage() {
        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$lookup: {from: 'coll', let: 'abc', pipeline: [], as: 'data'}")).first())
            .withMessageContaining("Command failed with error 9 (FailedToParse): '$lookup argument 'let: \"abc\"' must be an object, is type string'");

        collection.insertOne(json("_id: 1"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$lookup: {from: 'coll', let: {}, pipeline: 'abc', as: 'data'}")).first())
            .withMessageContaining("Command failed with error 14 (TypeMismatch): ''pipeline' option must be specified as an array'");
    }

    @Test
    public void testAggregateWithLookupAndPipeline() {
        MongoCollection<Document> ordersCollection = db.getCollection("orders");
        MongoCollection<Document> itemsCollection = db.getCollection("items");

        List<Document> pipeline = jsonList("$lookup: {from: 'items'," +
            " pipeline: [{$project: {item: 1, _id: 0}}]," +
            " as: 'items'}");

        assertThat(ordersCollection.aggregate(pipeline)).isEmpty();

        ordersCollection.insertOne(json("_id: 1, order: 100"));
        ordersCollection.insertOne(json("_id: 2, order: 101"));
        ordersCollection.insertOne(json("_id: 3, order: 102"));

        itemsCollection.insertOne(json("_id: 1, item: 'A'"));
        itemsCollection.insertOne(json("_id: 2, item: 'B'"));

        assertThat(ordersCollection.aggregate(pipeline))
            .containsExactly(
                json("_id: 1, order: 100, items: [{item: 'A'}, {item: 'B'}]"),
                json("_id: 2, order: 101, items: [{item: 'A'}, {item: 'B'}]"),
                json("_id: 3, order: 102, items: [{item: 'A'}, {item: 'B'}]")
            );
    }

    @Test
    public void testAggregateWithLookupAndUncorrelatedSubqueries() {
        MongoCollection<Document> ordersCollection = db.getCollection("orders");
        MongoCollection<Document> warehousesCollection = db.getCollection("warehouses");

        List<Document> pipeline = jsonList("$lookup: {from: 'warehouses'," +
            " let: { order_item: '$item', order_qty: '$ordered' }," +
            " pipeline: [{$match:\n" +
            "                 {$expr:\n" +
            "                    {$and:\n" +
            "                       [\n" +
            "                         {$eq: ['$stock_item',  '$$order_item']},\n" +
            "                         {$gte: ['$instock', '$$order_qty']}\n" +
            "                       ]\n" +
            "                    }\n" +
            "                 }\n" +
            "              },\n" +
            "              {$project: {stock_item: 0, _id: 0}}]," +
            " as: 'stockdata'}");

        assertThat(ordersCollection.aggregate(pipeline)).isEmpty();

        ordersCollection.insertOne(json("_id: 1, item: 'almonds', price: 12, ordered: 2"));
        ordersCollection.insertOne(json("_id: 2, item: 'pecans', price: 20, ordered: 1"));
        ordersCollection.insertOne(json("_id: 3, item: 'pecans', price: 10, ordered: 60"));

        warehousesCollection.insertOne(json("_id: 1, stock_item: 'almonds', warehouse: 'A', instock: 120"));
        warehousesCollection.insertOne(json("_id: 2, stock_item: 'pecans', warehouse: 'A', instock: 80"));
        warehousesCollection.insertOne(json("_id: 3, stock_item: 'almonds', warehouse: 'B', instock: 60"));
        warehousesCollection.insertOne(json("_id: 4, stock_item: 'cookies', warehouse: 'B', instock: 40"));
        warehousesCollection.insertOne(json("_id: 5, stock_item: 'cookies', warehouse: 'A', instock: 80"));

        assertThat(ordersCollection.aggregate(pipeline))
            .containsExactly(
                json("_id: 1, item: 'almonds', price: 12, ordered: 2, stockdata: [{instock: 120, warehouse: 'A'}, {instock: 60, warehouse: 'B'}]"),
                json("_id: 2, item: 'pecans', price: 20, ordered: 1, stockdata: [{instock: 80, warehouse: 'A'}]"),
                json("_id: 3, item: 'pecans', price: 10, ordered: 60, stockdata: [{instock: 80, warehouse: 'A'}]")
            );
    }

    @Test
    public void testAggregateWithReplaceRoot() {
        List<Document> pipeline = jsonList("$replaceRoot: { newRoot: '$a.b' }");

        assertThat(collection.aggregate(pipeline)).isEmpty();
        collection.insertOne(json("_id: 1, a: { b: { c: 10 } }"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("c: 10"));
    }

    @Test
    public void testAggregateWithIllegalReplaceRoot() {
        List<Document> pipeline = jsonList("$replaceRoot: { newRoot: '$a.b' }");

        collection.insertOne(json("_id: 1, a: { b: 10 }, c: 123"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("Command failed with error 40228 (Location40228): " +
                "''newRoot' expression must evaluate to an object, but resulting value was: 10." +
                " Type of resulting value: 'int'.")
            .withMessageContaining("a: {b: 10}");
    }

    @Test
    public void testAggregateWithProjectingReplaceRoot() {
        List<Document> pipeline = jsonList("$replaceRoot: { newRoot: { x: '$a.b' } }");

        assertThat(collection.aggregate(pipeline)).isEmpty();
        collection.insertOne(json("_id: 1, a: { b: { c: 10 } }"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("x: { c: 10 }"));
    }

    @Test
    public void testAggregateWithMergeObjects() throws Exception {
        MongoCollection<Document> orders = db.getCollection("orders");

        orders.insertOne(json("_id: 1, item: 'abc', 'price': 12, ordered: 2"));
        orders.insertOne(json("_id: 2, item: 'jkl', 'price': 20, ordered: 1"));

        MongoCollection<Document> items = db.getCollection("items");

        items.insertOne(json("_id: 1, item: 'abc', description: 'product 1', instock: 120"));
        items.insertOne(json("_id: 2, item: 'def', description: 'product 2', instock: 80"));
        items.insertOne(json("_id: 3, item: 'jkl', description: 'product 3', instock: 60"));

        List<Document> pipeline = jsonList(
            "$lookup: {from: 'items', localField: 'item', foreignField: 'item', as: 'fromItems'}",
            "$replaceRoot: {newRoot: {$mergeObjects: [{$arrayElemAt: ['$fromItems', 0 ]}, '$$ROOT']}}",
            "$project: { fromItems: 0 }"
        );

        assertThat(orders.aggregate(pipeline))
            .containsExactly(
                json("_id: 1, description: 'product 1', instock: 120, item: 'abc', ordered: 2, price: 12"),
                json("_id: 2, description: 'product 3', instock: 60, item: 'jkl', ordered: 1, price: 20")
            );
    }

    @Test
    public void testAggregateWithSortByCount() throws Exception {
        collection.insertOne(json("_id: 1, item: 'abc', 'price': 12, ordered: 2"));
        collection.insertOne(json("_id: 2, item: 'jkl', 'price': 20, ordered: 1"));
        collection.insertOne(json("_id: 3, item: 'jkl', 'price': 20, ordered: 7"));
        collection.insertOne(json("_id: 4, item: 'jkl', 'price': 40, ordered: 3"));
        collection.insertOne(json("_id: 5, item: 'abc', 'price': 90, ordered: 5"));
        collection.insertOne(json("_id: 6"));
        collection.insertOne(json("_id: 7, item: null"));
        collection.insertOne(json("_id: 8, item: 'aaa'"));
        collection.insertOne(json("_id: 9, item: 'abc'"));
        collection.insertOne(json("_id: 10, item: 'abc'"));

        List<Document> pipeline = jsonList("$sortByCount: '$item'");

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("_id: 'abc', count: 4"),
                json("_id: 'jkl', count: 3"),
                json("_id: null, count: 2"),
                json("_id: 'aaa', count: 1")
            );
    }

    @Test
    public void testAggregateWithGraphLookup() {
        collection.insertOne(json("_id: 1, name: 'folderA'"));
        collection.insertOne(json("_id: 2, name: 'subfolderA1', parent: 1"));
        collection.insertOne(json("_id: 3, name: 'File A11', parent: 2"));
        collection.insertOne(json("_id: 4, name: 'File A12', parent: 2"));
        collection.insertOne(json("_id: 5, name: 'subfolderA2', parent: 1"));
        collection.insertOne(json("_id: 6, name: 'File A21', parent: 5"));
        collection.insertOne(json("_id: 7, name: 'folderB'"));
        collection.insertOne(json("_id: 8, name: 'subfolderB1', parent: 7"));
        collection.insertOne(json("_id: 9, name: 'File B11', parent: 8"));

        List<Document> pipeline = jsonList("$match: {name: {$regex: 'File A1.*'}}",
            "$graphLookup: {from: 'testcoll', startWith: '$parent', connectFromField: 'parent', " +
                "connectToField: '_id', as: 'hierarchy', depthField: 'depth'}");

        assertThat(collection.aggregate(pipeline).map(withSortedDocuments("hierarchy")).map(Document::toJson))
            .containsExactly(
                "{\"_id\": 3, \"name\": \"File A11\", \"parent\": 2, \"hierarchy\": [{\"_id\": 1, \"name\": \"folderA\", \"depth\": {\"$numberLong\": \"1\"}}, {\"_id\": 2, \"name\": \"subfolderA1\", \"parent\": 1, \"depth\": {\"$numberLong\": \"0\"}}]}",
                "{\"_id\": 4, \"name\": \"File A12\", \"parent\": 2, \"hierarchy\": [{\"_id\": 1, \"name\": \"folderA\", \"depth\": {\"$numberLong\": \"1\"}}, {\"_id\": 2, \"name\": \"subfolderA1\", \"parent\": 1, \"depth\": {\"$numberLong\": \"0\"}}]}"
            );
    }

    @Test
    public void testObjectToArrayExpression() throws Exception {
        List<Document> pipeline = jsonList("$project: {_id: 1, a: {$objectToArray: '$value'}}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, value: 1"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("Command failed with error 40390 (Location40390): '$objectToArray requires a document input, found: int'");

        collection.replaceOne(json("_id: 1"), json("_id: 1, value: {a: 1, b: 'foo', c: {x: 10}}"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("_id: 1, a: [{k: 'a', v: 1}, {k: 'b', v: 'foo'}, {k: 'c', v: {x: 10}}]")
            );

        Document illegalQuery = json("$project: {_id: 1, a: {$objectToArray: ['$value', 1]}}");
        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(Collections.singletonList(illegalQuery)).first())
            .withMessageContaining("Command failed with error 16020 (Location16020): 'Expression $objectToArray takes exactly 1 arguments. 2 were passed in.'");
    }

    @Test
    public void testArrayToObjectExpression() throws Exception {
        collection.insertOne(TestUtils.json("_id: 1, a: 1, b: 'xyz', kv: [['a', 'b'], ['c', 'd']]"));

        assertThat(collection.aggregate(jsonList("$project: {_id: 1, x: {$arrayToObject: {$literal: [['a', 'foo']]}}}")))
            .containsExactly(json("_id: 1, x: {a: 'foo'}"));

        assertThat(collection.aggregate(jsonList("$project: {_id: 1, x: {$arrayToObject: '$kv'}}")))
            .containsExactly(json("_id: 1, x: {a: 'b', c: 'd'}"));

        assertThat(collection.aggregate(jsonList("$project: {_id: 1, x: {$arrayToObject: {$literal: [{k: 'k1', v: 'v1'}, {k: 'k2', v: 'v2'}]}}}")))
            .containsExactly(json("_id: 1, x: {k1: 'v1', k2: 'v2'}"));

        assertThat(collection.aggregate(jsonList("$project: {_id: 1, x: {$arrayToObject: {$literal: [{k: 'k1', v: 'v1'}, {k: 'k1', v: 'v2'}]}}}")))
            .containsExactly(json("_id: 1, x: {k1: 'v2'}"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {_id: 1, x: {$arrayToObject: 'illegal-type'}}")).first())
            .withMessageContaining("Command failed with error 40386 (Location40386): 'Failed to optimize pipeline :: caused by :: $arrayToObject requires an array input, found: string'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {_id: 1, x: {$arrayToObject: []}}")).first())
            .withMessageContaining("Command failed with error 16020 (Location16020): 'Expression $arrayToObject takes exactly 1 arguments. 0 were passed in.'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {_id: 1, x: {$arrayToObject: {$literal: [['foo']]}}}}")).first())
            .withMessageContaining("Command failed with error 40397 (Location40397): 'Failed to optimize pipeline :: caused by :: $arrayToObject requires an array of size 2 arrays,found array of size: 1'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {_id: 1, x: {$arrayToObject: {$literal: [123, 456]}}}}")).first())
            .withMessageContaining("Command failed with error 40398 (Location40398): 'Failed to optimize pipeline :: caused by :: Unrecognised input type format for $arrayToObject: int'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {_id: 1, x: {$arrayToObject: {$literal: [[123, 456]]}}}}")).first())
            .withMessageContaining("Command failed with error 40395 (Location40395): 'Failed to optimize pipeline :: caused by :: $arrayToObject requires an array of key-value pairs, where the key must be of type string. Found key type: int'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {_id: 1, x: {$arrayToObject: {$literal: [{}]}}}}")).first())
            .withMessageContaining("Command failed with error 40392 (Location40392): 'Failed to optimize pipeline :: caused by :: $arrayToObject requires an object keys of 'k' and 'v'. Found incorrect number of keys:0'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {_id: 1, x: {$arrayToObject: {$literal: [{k: 123, v: 'value'}]}}}}")).first())
            .withMessageContaining("Command failed with error 40394 (Location40394): 'Failed to optimize pipeline :: caused by :: $arrayToObject requires an object with keys 'k' and 'v', where the value of 'k' must be of type string. Found type: int'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {_id: 1, x: {$arrayToObject: {$literal: [{k: 'key', z: 'value'}]}}}}")).first())
            .withMessageContaining("Command failed with error 40393 (Location40393): 'Failed to optimize pipeline :: caused by :: $arrayToObject requires an object with keys 'k' and 'v'. Missing either or both keys from: {k: \"key\", z: \"value\"}'");
    }

    @Test
    public void testAggregateWithReduceOperation() throws Exception {
        collection.insertOne(json("_id: 1"));

        List<Document> pipeline = jsonList("$project: {}");

        Utils.changeSubdocumentValue(pipeline.get(0), "$project.res",
            json("$reduce: {input: ['a', 'b', 'c']," +
                " initialValue: ''," +
                " in: {$concat: ['$$value', '$$this'] }}"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("_id: 1, res: 'abc'"));

        Utils.changeSubdocumentValue(pipeline.get(0), "$project.res",
            json("$reduce: {input: [1, 2, 3, 4]," +
                " initialValue: { sum: 5, product: 2 }," +
                " in: {\n" +
                "     sum: {$add: ['$$value.sum', '$$this']},\n" +
                "     product: {$multiply: ['$$value.product', '$$this']}\n" +
                " }}"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("_id: 1, res: {sum: 15, product: 48}"));
    }

    @Test
    public void testAggregateWithMatchProjectReduceConcatAndCond() throws Exception {
        collection.insertOne(json("_id: 1, name: 'Melissa', hobbies: ['softball', 'drawing', 'reading']"));
        collection.insertOne(json("_id: 2, name: 'Brad', hobbies: ['gaming', 'skateboarding']"));
        collection.insertOne(json("_id: 3, name: 'Scott', hobbies: ['basketball', 'music', 'fishing']"));
        collection.insertOne(json("_id: 4, name: 'Tracey', hobbies: ['acting', 'yoga']"));
        collection.insertOne(json("_id: 5, name: 'Josh', hobbies: ['programming']"));
        collection.insertOne(json("_id: 6, name: 'Claire'"));

        List<Document> pipeline = jsonList("$match: {hobbies: {$gt: []}}",
            "$project: {\n" +
                "  name: 1,\n" +
                "  bio: {\n" +
                "    $reduce: {\n" +
                "      input: '$hobbies',\n" +
                "      initialValue: 'My hobbies include:',\n" +
                "      in: {\n" +
                "        $concat: [\n" +
                "          '$$value',\n" +
                "          {\n" +
                "            $cond: {\n" +
                "              if: { $eq: ['$$value', 'My hobbies include:']},\n" +
                "              then: ' ',\n" +
                "              else: ', '\n" +
                "            }\n" +
                "          },\n" +
                "          '$$this'\n" +
                "        ]\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}");

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("_id: 1, name: 'Melissa', bio: 'My hobbies include: softball, drawing, reading'"),
                json("_id: 2, name: 'Brad', bio: 'My hobbies include: gaming, skateboarding'"),
                json("_id: 3, name: 'Scott', bio: 'My hobbies include: basketball, music, fishing'"),
                json("_id: 4, name: 'Tracey', bio: 'My hobbies include: acting, yoga'"),
                json("_id: 5, name: 'Josh', bio: 'My hobbies include: programming'")
            );
    }

    @Test
    public void testAggregateWithBucketStage() throws Exception {
        collection.insertOne(json("_id: 1, title: 'The Pillars of Society', artist: 'Grosz', year: 1926, price: 199.99"));
        collection.insertOne(json("_id: 2, title: 'Melancholy III', artist: 'Munch', year: 1902, price: 280.00"));
        collection.insertOne(json("_id: 3, title: 'Dancer', artist: 'Miro', year: 1925, price: 76.04"));
        collection.insertOne(json("_id: 4, title: 'The Great Wave off Kanagawa', artist: 'Hokusai', price: 167.30"));
        collection.insertOne(json("_id: 5, title: 'The Persistence of Memory', artist: 'Dali', year: 1931, price: 483.00"));
        collection.insertOne(json("_id: 6, title: 'Composition VII', artist: 'Kandinsky', year: 1913, price: 385.00"));
        collection.insertOne(json("_id: 7, title: 'The Scream', artist: 'Munch', year: 1893"));
        collection.insertOne(json("_id: 8, title: 'Blue Flower', artist: 'O’Keefe', year: 1918, price: 118.42"));

        List<Document> pipeline = jsonList("$bucket: {\n" +
            "  groupBy: '$price',\n" +
            "  boundaries: [0, 200, 400],\n" +
            "  default: 'Other',\n" +
            "  output: {\n" +
            "    count: { $sum: 1 },\n" +
            "    titles: { $push: '$title' }\n" +
            "  }\n" +
            "}");

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("_id: 0, count: 4, titles: ['The Pillars of Society', 'Dancer', 'The Great Wave off Kanagawa', 'Blue Flower']"),
                json("_id: 200, count: 2, titles: ['Melancholy III', 'Composition VII']"),
                json("_id: 'Other', count: 2, titles: ['The Persistence of Memory', 'The Scream']")
            );

        List<Document> pipelineWithoutDefault = jsonList("$match: {price: {$lt: 400}}",
            "$bucket: {\n" +
                "  groupBy: '$price',\n" +
                "  boundaries: [0, 200, 400],\n" +
                "  output: {count: { $sum: 1 }}\n" +
                "}");

        assertThat(collection.aggregate(pipelineWithoutDefault))
            .containsExactly(
                json("_id: 0, count: 4"),
                json("_id: 200, count: 2")
            );

        List<Document> pipelineWithoutOutput = jsonList("$match: {price: {$lt: 400}}",
            "$bucket: {\n" +
                "  groupBy: '$price',\n" +
                "  boundaries: [0, 200.0, 400]" +
                "}");

        assertThat(collection.aggregate(pipelineWithoutOutput))
            .containsExactly(
                json("_id: 0, count: 4"),
                json("_id: 200.0, count: 2")
            );

        List<Document> pipelineWithAlphabeticBoundaries = jsonList("$bucket: {\n" +
            "  groupBy: {$toLower: '$artist'},\n" +
            "  boundaries: ['a', 'd', 'g', 'j'],\n" +
            "  default: null" +
            "}");

        assertThat(collection.aggregate(pipelineWithAlphabeticBoundaries))
            .containsExactly(
                json("_id: null, count: 5"),
                json("_id: 'd', count: 1"),
                json("_id: 'g', count: 2")
            );
    }

    @Test
    public void testAggregateWithIllegalBucketStage() throws Exception {
        collection.insertOne(json("_id: 1"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$bucket: {groupBy: '$_id', boundaries: [100, 200, 400]}")).first())
            .withMessageContaining("Command failed with error 40066 (Location40066): '$switch could not find a matching branch for an input, and no default was specified.'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$bucket: {groupBy: '$_id', boundaries: [0, 400], default: 200}")).first())
            .withMessageContaining("Command failed with error 40199 (Location40199): 'The $bucket 'default' field must be less than the lowest boundary or greater than or equal to the highest boundary.'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$bucket: {groupBy: '$_id', boundaries: [0, 400, 200]}")).first())
            .withMessageContaining("Command failed with error 40194 (Location40194): 'The 'boundaries' option to $bucket must be sorted, but elements 1 and 2 are not in ascending order (400 is not less than 200).'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$bucket: {groupBy: '$_id', boundaries: [0, 400], output: 'a'}")).first())
            .withMessageContaining("Command failed with error 40196 (Location40196): 'The $bucket 'output' field must be an object, but found type: string.'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$bucket: {groupBy: '$_id', boundaries: [0]}")).first())
            .withMessageContaining("Command failed with error 40192 (Location40192): 'The $bucket 'boundaries' field must have at least 2 values, but found 1 value(s).'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$bucket: {groupBy: '$_id', boundaries: 'abc'}")).first())
            .withMessageContaining("Command failed with error 40200 (Location40200): 'The $bucket 'boundaries' field must be an array, but found type: string.");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$bucket: {groupBy: [1, 2], boundaries: 'abc'}")).first())
            .withMessageContaining("Command failed with error 40202 (Location40202): 'The $bucket 'groupBy' field must be defined as a $-prefixed path or an expression, but found: [ 1, 2 ].'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$bucket: {groupBy: '$_id'}")).first())
            .withMessageContaining("Command failed with error 40198 (Location40198): '$bucket requires 'groupBy' and 'boundaries' to be specified.'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$bucket: {}")).first())
            .withMessageContaining("Command failed with error 40198 (Location40198): '$bucket requires 'groupBy' and 'boundaries' to be specified.'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$bucket: {groupBy: '$_id', boundaries: ['abc', 123]}")).first())
            .withMessageContaining("Command failed with error 40193 (Location40193): 'All values in the the 'boundaries' option to $bucket must have the same type. Found conflicting types string and int.'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$bucket: {groupBy: '$_id', boundaries: [0, null]}")).first())
            .withMessageContaining("Command failed with error 40193 (Location40193): 'All values in the the 'boundaries' option to $bucket must have the same type. Found conflicting types int and null.'");
    }

    @Test
    public void testAggregateWithFacetStage() throws Exception {
        collection.insertOne(json("_id: 1, title: 'The Pillars of Society', price: 199.99, tags: ['painting', 'Expressionism']"));
        collection.insertOne(json("_id: 2, title: 'Melancholy III', price: 280.00, tags: ['Expressionism']"));
        collection.insertOne(json("_id: 3, title: 'Dancer', price: 76.04, tags: ['oil', 'painting']"));
        collection.insertOne(json("_id: 4, title: 'The Great Wave off Kanagawa', price: 167.30, tags: ['woodblock']"));
        collection.insertOne(json("_id: 5, title: 'The Persistence of Memory', price: 483.00, tags: ['painting', 'oil']"));
        collection.insertOne(json("_id: 6, title: 'Composition VII', price: 385.00, tags: ['oil', 'painting', 'abstract']"));
        collection.insertOne(json("_id: 7, title: 'The Scream', tags: ['Expressionism', 'painting', 'oil']"));
        collection.insertOne(json("_id: 8, title: 'Blue Flower', price: 118.42, tags: ['abstract', 'painting']"));

        List<Document> pipeline = jsonList("$facet: {\n" +
            "  'categorizedByTags': [\n" +
            "    { $unwind: '$tags' },\n" +
            "    { $sortByCount: '$tags' }\n" +
            "  ],\n" +
            "  'categorizedByPrice': [\n" +
            "    { $match: { price: { $exists: 1 } } },\n" +
            "    {\n" +
            "      $bucket: {\n" +
            "        groupBy: '$price',\n" +
            "        boundaries: [0, 150, 200, 300, 400],\n" +
            "        default: 'Other',\n" +
            "        output: {\n" +
            "          'count': { $sum: 1 },\n" +
            "          'titles': { $push: '$title' }\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  ]\n" +
            "}");

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("'categorizedByPrice': [\n" +
                    "    {\n" +
                    "      _id: 0,\n" +
                    "      count: 2,\n" +
                    "      titles: ['Dancer', 'Blue Flower']\n" +
                    "    },\n" +
                    "    {\n" +
                    "      _id: 150,\n" +
                    "      count: 2,\n" +
                    "      titles: ['The Pillars of Society', 'The Great Wave off Kanagawa']\n" +
                    "    },\n" +
                    "    {\n" +
                    "      _id: 200,\n" +
                    "      count: 1,\n" +
                    "      titles: ['Melancholy III']\n" +
                    "    },\n" +
                    "    {\n" +
                    "      _id: 300,\n" +
                    "      count: 1,\n" +
                    "      titles: ['Composition VII']\n" +
                    "    },\n" +
                    "    {\n" +
                    "      _id: 'Other',\n" +
                    "      count: 1,\n" +
                    "      titles: ['The Persistence of Memory']\n" +
                    "    }\n" +
                    "  ],\n" +
                    "  'categorizedByTags': [\n" +
                    "    { _id: 'painting', count: 6 },\n" +
                    "    { _id: 'oil', count: 4 },\n" +
                    "    { _id: 'Expressionism', count: 3 },\n" +
                    "    { _id: 'abstract', count: 2 },\n" +
                    "    { _id: 'woodblock', count: 1 }\n" +
                    "  ]")
            );
    }

    @Test
    public void testAggregateWithMatchAndFacetStage() throws Exception {
        collection.insertOne(json("_id: 1, title: 'The Pillars of Society', price: 199.99, tags: ['painting', 'Expressionism']"));
        collection.insertOne(json("_id: 2, title: 'Melancholy III', price: 280.00, tags: ['Expressionism']"));
        collection.insertOne(json("_id: 3, title: 'Dancer', price: 76.04, tags: ['oil', 'painting']"));
        collection.insertOne(json("_id: 4, title: 'The Great Wave off Kanagawa', price: 167.30, tags: ['woodblock']"));
        collection.insertOne(json("_id: 5, title: 'The Persistence of Memory', price: 483.00, tags: ['painting']"));
        collection.insertOne(json("_id: 6, title: 'Composition VII', price: 385.00, tags: ['painting', 'abstract']"));
        collection.insertOne(json("_id: 7, title: 'The Scream', tags: ['Expressionism', 'painting', 'oil']"));
        collection.insertOne(json("_id: 8, title: 'Blue Flower', price: 118.42, tags: ['abstract', 'painting']"));

        List<Document> pipeline = jsonList(
            "$match: {price: {$gt: 300}}",
            "$facet: {\n" +
                "  'categorizedByTags': [\n" +
                "    { $unwind: '$tags' },\n" +
                "    { $sortByCount: '$tags' }\n" +
                "  ],\n" +
                "  'categorizedByPrice': [\n" +
                "    {$bucket: {groupBy: '$price', boundaries: [300, 400, 500]}}\n" +
                "  ]\n" +
                "}");

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("'categorizedByPrice': [\n" +
                    "    {_id: 300, count: 1},\n" +
                    "    {_id: 400, count: 1}\n" +
                    "  ],\n" +
                    "  'categorizedByTags': [\n" +
                    "    {_id: 'painting', count: 2},\n" +
                    "    {_id: 'abstract', count: 1}\n" +
                    "  ]")
            );
    }

    @Test
    public void testAggregateWithUnsetStage() throws Exception {
        List<Document> pipeline = jsonList("$unset: ['field1', 'fields.field2']");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, field1: 'value1', field2: 'value2'"));
        collection.insertOne(json("_id: 2, field1: 'value1', fields: { field2: 'value2'}"));
        collection.insertOne(json("_id: 3, fields: { field1: 'value1', field2: 'value2'}"));
        collection.insertOne(json("_id: 4, fields: {}"));
        collection.insertOne(json("_id: 5"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("_id: 1, field2: 'value2'"),
                json("_id: 2, fields: {}"),
                json("_id: 3, fields: { field1: 'value1' }"),
                json("_id: 4, fields: {}"),
                json("_id: 5")
            );
    }

    @Test
    public void testAggregateWithUnsetStage_illegalInput() throws Exception {
        collection.insertOne(json("_id: 1"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$unset: [123]")).first())
            .withMessageStartingWith("Command failed with error 31120 (Location31120): '$unset specification must be a string or an array containing only string values'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$unset: ['']")).first())
            .withMessageStartingWith("Command failed with error 40352 (Location40352): 'Invalid $project :: caused by :: FieldPath cannot be constructed with empty string'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$unset: ['field1', 123]")).first())
            .withMessageStartingWith("Command failed with error 31120 (Location31120): '$unset specification must be a string or an array containing only string values'");
    }

    @Test
    public void testAggregateWithIndexStats() throws Exception {
        List<Document> pipeline = jsonList("$indexStats: {}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));

        Document indexStats = CollectionUtils.getSingleElement(collection.aggregate(pipeline));
        assertThat(indexStats)
            .containsOnlyKeys("name", "key", "host", "accesses", "spec")
            .containsEntry("name", "_id_")
            .containsEntry("key", json("_id: 1"))
            .containsEntry("spec", json("key: {_id: 1}, name: '_id_', ns: 'testdb.testcoll', v: 2"));

        assertThat((Document) indexStats.get("accesses"))
            .containsEntry("ops", 0L);
    }

    @Test
    public void testAggregateWithOut() {
        List<Document> pipeline = jsonList(
            "$group: {_id: '$author', books: {$push: '$title'}}",
            "$out : 'authors'");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 8751, title: 'The Banquet', author: 'Dante', copies: 2"));
        collection.insertOne(json("_id: 8752, title: 'Divine Comedy', author: 'Dante', copies: 1"));
        collection.insertOne(json("_id: 8645, title: 'Eclogues', author: 'Dante', copies: 2"));
        collection.insertOne(json("_id: 7000, title: 'The Odyssey', author: 'Homer', copies: 10"));
        collection.insertOne(json("_id: 7020, title: 'Iliad', author: 'Homer', copies: 10"));

        List<Document> expectedDocuments = Arrays.asList(
            json("_id: 'Homer', books: ['The Odyssey', 'Iliad']"),
            json("_id: 'Dante', books: ['The Banquet', 'Divine Comedy', 'Eclogues']"));

        assertThat(collection.aggregate(pipeline))
            .containsExactlyInAnyOrderElementsOf(expectedDocuments);

        assertThat(db.getCollection("authors").find(json("")))
            .containsExactlyInAnyOrderElementsOf(expectedDocuments);

        // re-run will overwrite the existing collection

        assertThat(collection.aggregate(pipeline))
            .containsExactlyInAnyOrderElementsOf(expectedDocuments);

        assertThat(db.getCollection("authors").find(json("")))
            .containsExactlyInAnyOrderElementsOf(expectedDocuments);
    }

    @Test
    public void testAggregateWithOut_illegal() {
        collection.insertOne(json("_id: 8751, title: 'The Banquet', author: 'Dante', copies: 2"));

        assertThatExceptionOfType(BsonInvalidOperationException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$out : 123")).first())
            .withMessage("Value expected to be of type STRING is of unexpected type INT32");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$out : ''")).first())
            .withMessage("state should be: collectionName is not empty");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$out : 'some$collection'")).first())
            .withMessageContaining("Command failed with error 17385 (Location17385): 'Can't $out to special collection: some$collection'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$out : 'one'", "$out : 'other'")).first())
            .withMessageContaining("Command failed with error 40601 (Location40601): '$out can only be the final stage in the pipeline'");
    }

    private static Function<Document, Document> withSortedStringList(String key) {
        return document -> {
            @SuppressWarnings("unchecked")
            List<String> list = (List<String>) document.get(key);
            list.sort(ValueComparator.asc());
            return document;
        };
    }

    private static Function<Document, Document> withSortedDocuments(String key) {
        return document -> {
            @SuppressWarnings("unchecked")
            List<Document> list = (List<Document>) document.get(key);
            list.sort((o1, o2) -> {
                de.bwaldvogel.mongo.bson.Document d1 = toDocument(o1);
                de.bwaldvogel.mongo.bson.Document d2 = toDocument(o2);
                return ValueComparator.asc().compare(d1, d2);
            });
            return document;
        };
    }

    private static de.bwaldvogel.mongo.bson.Document toDocument(Document document) {
        return new de.bwaldvogel.mongo.bson.Document(document);
    }

}
