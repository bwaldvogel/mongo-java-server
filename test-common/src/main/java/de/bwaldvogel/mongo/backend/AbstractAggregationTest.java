package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.backend.TestUtils.date;
import static de.bwaldvogel.mongo.backend.TestUtils.instant;
import static de.bwaldvogel.mongo.backend.TestUtils.json;
import static de.bwaldvogel.mongo.backend.TestUtils.jsonList;
import static de.bwaldvogel.mongo.backend.TestUtils.toArray;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.fail;

import java.sql.Date;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.bson.BsonInvalidOperationException;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.mongodb.Function;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;

public abstract class AbstractAggregationTest extends AbstractTest {

    @BeforeEach
    void dropAllDatabasesBeforeEachTest() {
        dropAllDatabases();
    }

    @Test
    void testUnrecognizedAggregatePipelineStage() throws Exception {
        List<Document> pipeline = jsonList("$unknown: {}");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 40324 (Location40324): 'Unrecognized pipeline stage name: '$unknown'");
    }

    @Test
    void testIllegalAggregatePipelineStage() throws Exception {
        List<Document> pipeline = jsonList("$unknown: {}, bar: 1");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 40323 (Location40323): 'A pipeline stage specification object must contain exactly one field.'");
    }

    @Test
    void testAggregateWithMissingCursor() throws Exception {
        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> db.runCommand(json("aggregate: 'collection', pipeline: [{$match: {}}]")))
            .withMessageContaining("Command execution failed on MongoDB server with error 9 (FailedToParse): 'The 'cursor' option is required, except for aggregate with the explain argument'");
    }

    @Test
    void testAggregateWithIllegalPipeline() throws Exception {
        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> db.runCommand(json("aggregate: 'collection', cursor: {}, pipeline: 123")))
            .withMessageContaining("Command execution failed on MongoDB server with error 14 (TypeMismatch): ''pipeline' option must be specified as an array'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> db.runCommand(json("aggregate: 'collection', cursor: {}, pipeline: [1, 2, 3]")))
            .withMessageContaining("Command execution failed on MongoDB server with error 14 (TypeMismatch): 'Each element of the 'pipeline' array must be an object");
    }

    @Test
    void testAggregateWithComplexGroupBySumPipeline() throws Exception {
        Document query = json("_id: null, n: {$sum: 1}, sumOfA: {$sum: '$a'}, sumOfB: {$sum: '$b.value'}");
        List<Document> pipeline = List.of(new Document("$group", query));

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, a: 30, b: {value: 20}"));
        collection.insertOne(json("_id: 2, a: 15, b: {value: 10.5}"));
        collection.insertOne(json("_id: 3, b: {value: 1}"));
        collection.insertOne(json("_id: 4, a: {value: 5}"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("_id: null, n: 4, sumOfA: 45, sumOfB: 31.5"));
    }

    @Test
    void testAggregateWithGroupByMinAndMax() throws Exception {
        Document query = json("_id: null, minA: {$min: '$a'}, maxB: {$max: '$b.value'}, maxC: {$max: '$c'}, minC: {$min: '$c'}");
        List<Document> pipeline = List.of(new Document("$group", query));

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
    void testAggregateWithGroupByMinAndMaxOnArrayField() throws Exception {
        Document query = json("_id: null, min: {$min: '$v'}, max: {$max: '$v'}");
        List<Document> pipeline = List.of(new Document("$group", query));

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, v: [10, 20, 30]"));
        collection.insertOne(json("_id: 2, v: [3, 40]"));
        collection.insertOne(json("_id: 3, v: [11, 25]"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("_id: null, max: [11, 25], min: [3, 40]"));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/68
    @Test
    void testAggregateWithGroupByMinAndMaxOnArrayFieldAndNonArrayFields() throws Exception {
        Document query = json("_id: null, min: {$min: '$v'}, max: {$max: '$v'}");
        List<Document> pipeline = List.of(new Document("$group", query));

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
    void testAggregateWithGroupByNonExistingMinAndMax() throws Exception {
        Document query = json("_id: null, minOfA: {$min: '$doesNotExist'}, maxOfB: {$max: '$doesNotExist'}");
        List<Document> pipeline = List.of(new Document("$group", query));

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, a: 30, b: {value: 20}"));
        collection.insertOne(json("_id: 2, a: 15, b: {value: 10}"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("_id: null, minOfA: null, maxOfB: null"));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/104#issuecomment-548151945
    @Test
    void testMinMaxAvgProjectionOfArrayValues() throws Exception {
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
    void testMinMaxAvgProjectionOfNonArrayValue() throws Exception {
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
    void testMinMaxAvgProjectionWithTwoParameters() throws Exception {
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
    void testMinMaxAvgProjectionWithOneParameter() throws Exception {
        List<Document> pipeline = jsonList("$project: {min: {$min: 'abc'}, max: {$max: 'def'}, avg: {$avg: 10}}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("_id: 1, min: 'abc', max: 'def', avg: 10.0"));
    }

    @Test
    void testAggregateWithUnknownGroupOperator() throws Exception {
        Document query = json("_id: null, n: {$foo: 1}");
        List<Document> pipeline = List.of(new Document("$group", query));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 15952 (Location15952): 'unknown group operator '$foo''");
    }

    @Test
    void testAggregateWithTooManyGroupOperators() throws Exception {
        Document query = json("_id: null, n: {$sum: 1, $max: 1}");
        List<Document> pipeline = List.of(new Document("$group", query));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 40238 (Location40238): 'The field 'n' must specify one accumulator'");
    }

    @Test
    void testAggregateWithEmptyPipeline() throws Exception {
        assertThat(collection.aggregate(Collections.emptyList())).isEmpty();

        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));

        assertThat(collection.aggregate(Collections.emptyList()))
            .containsExactly(json("_id: 1"), json("_id: 2"));
    }

    @Test
    void testAggregateWithMissingIdInGroupSpecification() throws Exception {
        List<Document> pipeline = List.of(new Document("$group", json("n: {$sum: 1}")));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 15955 (Location15955): 'a group specification must include an _id'");
    }

    @Test
    void testAggregateWithGroupBySumPipeline() throws Exception {
        Document query = json("_id: null, n: {$sum: 1}");
        List<Document> pipeline = List.of(new Document("$group", query));

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
    void testAggregateWithGroupByAvg() throws Exception {
        Document query = json("_id: null, avg: {$avg: 1}");
        List<Document> pipeline = List.of(new Document("$group", query));

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
    void testAggregateWithGroupByKey() throws Exception {
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
    void testAggregateWithGroupByNumberEdgeCases() throws Exception {
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
    void testAggregateWithGroupByDocuments() throws Exception {
        String groupBy = "$group: {_id: '$a', count: {$sum: 1}}";
        String sort = "$sort: {_id: 1}";
        List<Document> pipeline = List.of(json(groupBy), json(sort));

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
    void testAggregateWithGroupByIllegalKey() throws Exception {
        collection.insertOne(json("_id:  1, a: 1"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$group: {_id: '$a.'}")).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 40353 (Location40353): 'FieldPath must not end with a '.'.'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$group: {_id: '$a..1'}")).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 15998 (Location15998): 'FieldPath field names may not be empty strings.'");
    }

    @Test
    void testAggregateWithSimpleExpressions() throws Exception {
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
    void testAggregateWithMultipleExpressionsInKey() throws Exception {
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
    void testAggregateWithAddToSet() throws Exception {
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
    void testAggregateWithEmptyAddToSet() throws Exception {
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
    void testAggregateWithAddToSetAndMissingValue() throws Exception {
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
    void testAggregateWithAdd() throws Exception {
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
    void testAggregateWithSort() throws Exception {
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
    void testAggregateWithProjection() throws Exception {
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
    void testAggregateWithNestedExclusiveProjection() throws Exception {
        List<Document> pipeline = jsonList("$project: {_id: 0, 'x.b': 0}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, x: {a: 1, b: 2, c: 3}"));
        collection.insertOne(json("_id: 2, x: 20"));
        collection.insertOne(json("_id: 3"));
        collection.insertOne(json("_id: 4, x: ['abc', {a: 1, b: 2}, {a: 2, b: 3, c: 4}]"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("x: {a: 1, c: 3}"),
                json("x: 20"),
                json(""),
                json("x: ['abc', {a: 1}, {a: 2, c: 4}]")
            );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/121
    @Test
    void testAggregateWithNestedExclusiveProjection_array() throws Exception {
        List<Document> pipeline = jsonList("$project: {_id: 0, 'a.b.c': 0}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, a: [{b: {c: 1}}]"));
        collection.insertOne(json("_id: 2, a: {b: [{x: 1, y: 2, c: 3}, {c: 4}]}"));
        collection.insertOne(json("_id: 3, a: [{b: {c: [1, 2, 3]}}]"));
        collection.insertOne(json("_id: 4, a: {b: {c: [1, 2, 3]}}"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("a: [{b: {}}]"),
                json("a: {b: [{x: 1, y: 2}, {}]}"),
                json("a: [{b: {}}]"),
                json("a: {b: {}}")
            );
    }

    @Test
    void testAggregateWithNestedInclusiveProjection() throws Exception {
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

    private static Stream<Arguments> aggregateWithProjectionArguments() {
        return Stream.of(
            Arguments.of("$project: {'x.b': 1, 'x.c': 1, 'x.d': 0, y: 0}",
                "Command execution failed on MongoDB server with error 31254 (Location31254): 'Invalid $project :: caused by :: Cannot do exclusion on field x.d in inclusion projection'"),

            Arguments.of("$project: {_id: 0, v: '$x.1.'}",
                "Command execution failed on MongoDB server with error 40353 (Location40353): 'Invalid $project :: caused by :: FieldPath must not end with a '.'.'"),

            Arguments.of("$project: {_id: 0, v: '$x..1'}",
                "Command execution failed on MongoDB server with error 15998 (Location15998): 'Invalid $project :: caused by :: FieldPath field names may not be empty strings.'"),

            Arguments.of("$project: 'abc'",
                "Command execution failed on MongoDB server with error 15969 (Location15969): '$project specification must be an object'")
        );
    }

    @ParameterizedTest
    @MethodSource("aggregateWithProjectionArguments")
    void testAggregateWithProjection_IllegalFieldPath(String project, String expectedMessage) throws Exception {
        collection.insertOne(json("_id: 1, x: 10"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList(project)).first())
            .withMessageStartingWith(expectedMessage);
    }

    // https://github.com/bwaldvogel/mongo-java-server/pull/189
    @Test
    void testAggregateWithProjection_arrayElemAt() throws Exception {
        collection.insertOne(json("_id: 1, items: [{foo: 'bar'}, {foo: 'bas'}, {foo: 'bat'}]"));
        collection.insertOne(json("_id: 2, items: [{}]"));
        collection.insertOne(json("_id: 3, items: [{foo: null}]"));
        collection.insertOne(json("_id: 4, items: []"));
        collection.insertOne(json("_id: 5"));

        assertThat(collection.aggregate(jsonList("$project: {value: {$arrayElemAt: ['$items.foo', 0]}}")))
            .containsExactly(
                json("_id: 1, value: 'bar'"),
                json("_id: 2"),
                json("_id: 3, value: null"),
                json("_id: 4"),
                json("_id: 5, value: null")
            );

        assertThat(collection.aggregate(jsonList("$project: {value: {$arrayElemAt: ['$items.foo', -1]}}")))
            .containsExactly(
                json("_id: 1, value: 'bat'"),
                json("_id: 2"),
                json("_id: 3, value: null"),
                json("_id: 4"),
                json("_id: 5, value: null")
            );

        assertThat(collection.aggregate(jsonList("$project: {value: {$arrayElemAt: ['$items.foo', 10]}}")))
            .containsExactly(
                json("_id: 1"),
                json("_id: 2"),
                json("_id: 3"),
                json("_id: 4"),
                json("_id: 5, value: null")
            );
    }

    @Test
    void testAggregateWithExpressionProjection() throws Exception {
        List<Document> pipeline = jsonList("$project: {_id: 0, idHex: {$toString: '$_id'}}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(new Document("_id", new ObjectId("abcd01234567890123456789")));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("idHex: 'abcd01234567890123456789'"));
    }

    @Test
    void testAggregateWithStrLenExpressionProjection() throws Exception {
        List<Document> pipeline = jsonList("$project: {_id: 0, lenCP: {$strLenCP: '$a'}, lenBytes: {$strLenBytes: '$a'}}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("a: 'cafétéria', b: 123"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("lenCP: 9, lenBytes: 11"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {len: {$strLenCP: '$x'}}")).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 34471 (Location34471): " +
                "'PlanExecutor error during aggregation :: caused by :: " +
                "$strLenCP requires a string argument, found: missing'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {len: {$strLenCP: '$b'}}")).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 34471 (Location34471): " +
                "'PlanExecutor error during aggregation :: caused by :: " +
                "$strLenCP requires a string argument, found: int'");
    }

    @Test
    void testAggregateWithSubstringExpressionProjection() throws Exception {
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
            .withMessageContaining("Command execution failed on MongoDB server with error 16020 (Location16020): 'Invalid $project :: caused by :: Expression $substrBytes takes exactly 3 arguments. 1 were passed in.'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {x: {$substr: ['abc', 'abc', 3]}}")).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 16034 (Location16034): 'Failed to optimize pipeline :: caused by :: $substrBytes:  starting index must be a numeric type (is BSON type string)");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {x: {$substr: ['abc', 3, 'abc']}}")).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 16035 (Location16035): 'Failed to optimize pipeline :: caused by :: $substrBytes:  length must be a numeric type (is BSON type string)");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {x: {$substrCP: 'abc'}}")).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 16020 (Location16020): 'Invalid $project :: caused by :: Expression $substrCP takes exactly 3 arguments. 1 were passed in.'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {x: {$substrCP: ['abc', 'abc', 3]}}")).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 34450 (Location34450): 'Failed to optimize pipeline :: caused by :: $substrCP: starting index must be a numeric type (is BSON type string)");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {x: {$substrCP: ['abc', 3, 'abc']}}")).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 34452 (Location34452): 'Failed to optimize pipeline :: caused by :: $substrCP: length must be a numeric type (is BSON type string)");
    }

    @Test
    void testAggregateWithSubstringUnicodeExpressionProjection() throws Exception {
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
    void testAggregateWithAddFields() throws Exception {
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
    void testAggregateWithMultipleMatches() throws Exception {
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
    void testAggregateWithLogicalAndInMatch() throws Exception {
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
    void testAggregateWithLogicalAndInMatchExpr() throws Exception {
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
    void testAggregateWithLogicalOrInMatchExpr() throws Exception {
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
    void testAggregateWithLogicalOrInMatch() throws Exception {
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
    void testAggregateWithCeil() throws Exception {
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
    void testAggregateWithNumericOperators() throws Exception {
        List<Document> pipeline = jsonList("$project: {a: 1, exp: {$exp: '$a'}, ln: {$ln: '$a'}, log10: {$log10: '$a'}, sqrt: {$sqrt: '$a'}}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, a: 1"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("_id: 1, a: 1, exp: 2.718281828459045, ln: 0.0, log10: 0.0, sqrt: 1.0"));
    }

    @Test
    void testAggregateWithCount() throws Exception {
        Document match = json("$match: {score: {$gt: 80}}");
        Document count = json("$count: 'passing_scores'");
        List<Document> pipeline = List.of(match, count);

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
    void testAggregateWithFirstAndLast() throws Exception {
        Document sort = json("$sort: { item: 1, date: 1 }");
        Document group = json("$group: {_id: '$item', firstSale: { $first: '$date' }, lastSale: { $last: '$date'} }");
        List<Document> pipeline = List.of(sort, group);

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
    void testAggregateWithFirstAndLast_Missing() throws Exception {
        Document group = json("$group: {_id: '$_id', first: { $first: '$field1' }, last: { $last: '$field2'} }");
        List<Document> pipeline = List.of(group);

        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2, field1: 'abc'"));
        collection.insertOne(json("_id: 3, field2: 'abc'"));
        collection.insertOne(json("_id: 4, field1: 'abc', field2: 'abc'"));

        assertThat(collection.aggregate(pipeline))
            .containsExactlyInAnyOrder(
                json("_id: 1, first: null, last: null"),
                json("_id: 2, first: 'abc', last: null"),
                json("_id: 3, first: null, last: 'abc'"),
                json("_id: 4, first: 'abc', last: 'abc'")
            );
    }

    @Test
    void testAggregateWithGroupingAndFirstMissing() throws Exception {
        Document sort = json("$sort: {_id: 1}");
        Document group = json("$group: {_id: '$group', first: { $first: '$field1' }}");
        List<Document> pipeline = List.of(sort, group);

        collection.insertOne(json("_id: 1, group: 1"));
        collection.insertOne(json("_id: 2, group: 1, field1: 'abc'"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("_id: 1, first: null")
            );
    }

    @Test
    void testAggregateWithGroupingAndLastMissing() throws Exception {
        Document sort = json("$sort: {_id: 1}");
        Document group = json("$group: {_id: '$group', last: { $last: '$field1' }}");
        List<Document> pipeline = List.of(sort, group);

        collection.insertOne(json("_id: 1, group: 1, field1: 'abc'"));
        collection.insertOne(json("_id: 2, group: 1"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("_id: 1, last: null")
            );
    }

    @Test
    void testAggregateWithPush() throws Exception {
        List<Document> pipeline = jsonList("$group: {_id: null, a: {$push: '$a'}, b: {$push: {v: '$b'}}, c: {$push: '$c'}}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, a: 10, b: 0.1"));
        collection.insertOne(json("_id: 2, a: 20, b: 0.2"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("_id: null, a: [10, 20], b: [{v: 0.1}, {v: 0.2}], c: []"));
    }

    @Test
    void testAggregateWithUndefinedVariable() throws Exception {
        List<Document> pipeline = jsonList("$project: {result: '$$UNDEFINED'}");

        collection.insertOne(json("_id: 1"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 17276 (Location17276): 'Invalid $project :: caused by :: Use of undefined variable: UNDEFINED'");
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/31
    @Test
    void testAggregateWithRootVariable() throws Exception {
        List<Document> pipeline = jsonList("$project: {_id: 0, doc: '$$ROOT', a: '$$ROOT.a', a_v: '$$ROOT.a.v'}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, a: {v: 10}"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("doc: {_id: 1, a: {v: 10}}, a: {v: 10}, a_v: 10"));
    }

    @Test
    void testAggregateWithRootVariable_IllegalFieldPath() throws Exception {
        collection.insertOne(json("_id: 1, x: 10"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {_id: '$$ROOT.a.'}")).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 40353 (Location40353): 'Invalid $project :: caused by :: FieldPath must not end with a '.'.'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {_id: '$$ROOT.a..1'}")).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 15998 (Location15998): 'Invalid $project :: caused by :: FieldPath field names may not be empty strings.'");
    }

    @Test
    void testAggregateWithSetOperations() throws Exception {
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
    void testAggregateWithComparisonOperations() throws Exception {
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
        pipeline = List.of(project);

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
        pipeline = List.of(project);

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
    void testAggregateWithSlice() throws Exception {
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
    void testAggregateWithSplit() throws Exception {
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

    // https://github.com/bwaldvogel/mongo-java-server/issues/214
    @Test
    void testAggregateWithSplitAndArrayElementAt() throws Exception {
        List<Document> pipeline = jsonList("$addFields: { pathSegments: { $split: ['$path', '/'] }}",
            "$project: { pathSegments: 1, firstSegment: { $arrayElemAt: ['$pathSegments', 0] }}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, path: 'path/to/file'"));
        collection.insertOne(json("_id: 2, path: '/path/to/file'"));
        collection.insertOne(json("_id: 3, path: '/path/to/file/'"));
        collection.insertOne(json("_id: 4"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("_id: 1, pathSegments: ['path', 'to', 'file'], firstSegment: 'path'"),
                json("_id: 2, pathSegments: ['', 'path', 'to', 'file'], firstSegment: ''"),
                json("_id: 3, pathSegments: ['', 'path', 'to', 'file', ''], firstSegment: ''"),
                json("_id: 4, pathSegments: null, firstSegment: null")
            );
    }

    @Test
    void testAggregateWithUnwind() throws Exception {
        testAggregateWithUnwind(json("$unwind: '$sizes'"));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/54
    @Test
    void testAggregateWithUnwind_Path() throws Exception {
        testAggregateWithUnwind(json("$unwind: {path: '$sizes'}"));
    }

    private void testAggregateWithUnwind(Document unwind) throws Exception {
        List<Document> pipeline = List.of(unwind);

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
    void testAggregateWithUnwind_preserveNullAndEmptyArrays() throws Exception {
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
    void testAggregateWithUnwind_IncludeArrayIndex() throws Exception {
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
    void testAggregateWithUnwind_IncludeArrayIndex_OverwriteExistingField() throws Exception {
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
    void testAggregateWithUnwind_IncludeArrayIndex_NestedIndexField() throws Exception {
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
    void testAggregateWithUnwind_preserveNullAndEmptyArraysAndIncludeArrayIndex() throws Exception {
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
    void testAggregateWithUnwind_subdocumentArray() throws Exception {
        List<Document> pipeline = List.of(json("$unwind: {path: '$items.sizes'}"));

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
    void testAggregateWithLookup() {
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
    void testAggregateWithLookup_collectionDoesNotExist() {
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
    void testAggregateWithIllegalLookupStage() {
        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$lookup: {from: 'coll', let: 'abc', pipeline: [], as: 'data'}")).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 9 (FailedToParse): '$lookup argument 'let: \"abc\"' must be an object, is type string'");

        collection.insertOne(json("_id: 1"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$lookup: {from: 'coll', let: {}, pipeline: 'abc', as: 'data'}")).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 14 (TypeMismatch): ''pipeline' option must be specified as an array'");
    }

    @Test
    void testAggregateWithLookupAndPipeline() {
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
    void testAggregateWithLookupAndUncorrelatedSubqueries() {
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
    void testLookupPersonWithSubscription() {
        MongoCollection<Document> personCollection = db.getCollection("Person");
        MongoCollection<Document> subscriptionCollection = db.getCollection("Subscription");

        personCollection.insertOne(json("""
              "_id": 666,
              "personId": 666,
              "Relation": [
                {
                  "organizationId": "org1",
                  "active": true
                },
                {
                  "organizationId": "org3",
                  "active": false
                }
              ]
            """));

        subscriptionCollection.insertOne(json("""
              "_id": 123,
              "organizationId": "org1",
              "Subscription": [
                {
                  "Id": 1,
                  "Name": "Netflix"
                },
                {
                  "Id": 12,
                  "Name": "Disney+"
                }
              ]
            """));
        subscriptionCollection.insertOne(json("""
              "_id": 456,
              "organizationId": "org2",
              "Subscription": [
                {
                  "Id": 2,
                  "Name": "HBO Max"
                },
                {
                  "Id": 11,
                  "Name": "SkyShowtime"
                }
              ]
            """));

        List<Document> pipeline = jsonList(
            "$match: { 'personId': 666 }",
            "$lookup: {from: 'Subscription', localField: 'Relation.organizationId', foreignField: 'organizationId', as: 'Subscriptions'}"
        );

        assertThat(personCollection.aggregate(pipeline))
            .containsExactly(json("""
                  "_id": 666,
                  "personId": 666,
                  "Relation": [
                    {
                      "organizationId": "org1",
                      "active": true
                    },
                    {
                      "organizationId": "org3",
                      "active": false
                    }
                  ],
                  "Subscriptions": [
                    {
                      "_id": 123,
                      "organizationId": "org1",
                      "Subscription": [
                        {
                          "Id": 1,
                          "Name": "Netflix"
                        },
                        {
                          "Id": 12,
                          "Name": "Disney+"
                        }
                      ]
                    }
                  ]
                """));
    }

    @Test
    void testAggregateWithReplaceRoot() {
        List<Document> pipeline = jsonList("$replaceRoot: { newRoot: '$a.b' }");

        assertThat(collection.aggregate(pipeline)).isEmpty();
        collection.insertOne(json("_id: 1, a: { b: { c: 10 } }"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("c: 10"));
    }

    @Test
    void testAggregateWithIllegalReplaceRoot() {
        List<Document> pipeline = jsonList("$replaceRoot: { newRoot: '$a.b' }");

        collection.insertOne(json("_id: 1, a: { b: 10 }, c: 123"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 40228 (Location40228): " +
                "'PlanExecutor error during aggregation :: caused by :: 'newRoot' expression must evaluate to an object, but resulting value was: 10." +
                " Type of resulting value: 'int'.")
            .withMessageContaining("a: {b: 10}");
    }

    @Test
    void testAggregateWithProjectingReplaceRoot() {
        List<Document> pipeline = jsonList("$replaceRoot: { newRoot: { x: '$a.b' } }");

        assertThat(collection.aggregate(pipeline)).isEmpty();
        collection.insertOne(json("_id: 1, a: { b: { c: 10 } }"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(json("x: { c: 10 }"));
    }

    @Test
    void testAggregateWithMergeObjects() throws Exception {
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
    void testAggregateWithSortByCount() throws Exception {
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
    void testAggregateWithGraphLookup() {
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
                "{\"_id\": 3, \"name\": \"File A11\", \"parent\": 2, \"hierarchy\": [{\"_id\": 1, \"name\": \"folderA\", \"depth\": 1}, {\"_id\": 2, \"name\": \"subfolderA1\", \"parent\": 1, \"depth\": 0}]}",
                "{\"_id\": 4, \"name\": \"File A12\", \"parent\": 2, \"hierarchy\": [{\"_id\": 1, \"name\": \"folderA\", \"depth\": 1}, {\"_id\": 2, \"name\": \"subfolderA1\", \"parent\": 1, \"depth\": 0}]}"
            );
    }

    @Test
    void testObjectToArrayExpression() throws Exception {
        List<Document> pipeline = jsonList("$project: {_id: 1, a: {$objectToArray: '$value'}}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1, value: 1"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 40390 (Location40390): 'PlanExecutor error during aggregation :: caused by :: $objectToArray requires a document input, found: int'");

        collection.replaceOne(json("_id: 1"), json("_id: 1, value: {a: 1, b: 'foo', c: {x: 10}}"));

        assertThat(collection.aggregate(pipeline))
            .containsExactly(
                json("_id: 1, a: [{k: 'a', v: 1}, {k: 'b', v: 'foo'}, {k: 'c', v: {x: 10}}]")
            );

        Document illegalQuery = json("$project: {_id: 1, a: {$objectToArray: ['$value', 1]}}");
        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(List.of(illegalQuery)).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 16020 (Location16020): 'Invalid $project :: caused by :: Expression $objectToArray takes exactly 1 arguments. 2 were passed in.'");
    }

    @Test
    void testArrayToObjectExpression() throws Exception {
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
            .withMessageContaining("Command execution failed on MongoDB server with error 40386 (Location40386): 'Failed to optimize pipeline :: caused by :: $arrayToObject requires an array input, found: string'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {_id: 1, x: {$arrayToObject: []}}")).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 16020 (Location16020): 'Invalid $project :: caused by :: Expression $arrayToObject takes exactly 1 arguments. 0 were passed in.'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {_id: 1, x: {$arrayToObject: {$literal: [['foo']]}}}}")).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 40397 (Location40397): 'Failed to optimize pipeline :: caused by :: $arrayToObject requires an array of size 2 arrays,found array of size: 1'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {_id: 1, x: {$arrayToObject: {$literal: [123, 456]}}}}")).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 40398 (Location40398): 'Failed to optimize pipeline :: caused by :: Unrecognised input type format for $arrayToObject: int'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {_id: 1, x: {$arrayToObject: {$literal: [[123, 456]]}}}}")).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 40395 (Location40395): 'Failed to optimize pipeline :: caused by :: $arrayToObject requires an array of key-value pairs, where the key must be of type string. Found key type: int'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {_id: 1, x: {$arrayToObject: {$literal: [{}]}}}}")).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 40392 (Location40392): 'Failed to optimize pipeline :: caused by :: $arrayToObject requires an object keys of 'k' and 'v'. Found incorrect number of keys:0'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {_id: 1, x: {$arrayToObject: {$literal: [{k: 123, v: 'value'}]}}}}")).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 40394 (Location40394): 'Failed to optimize pipeline :: caused by :: $arrayToObject requires an object with keys 'k' and 'v', where the value of 'k' must be of type string. Found type: int'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: {_id: 1, x: {$arrayToObject: {$literal: [{k: 'key', z: 'value'}]}}}}")).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 40393 (Location40393): 'Failed to optimize pipeline :: caused by :: $arrayToObject requires an object with keys 'k' and 'v'. Missing either or both keys from: {k: \"key\", z: \"value\"}'");
    }

    @Test
    void testAggregateWithReduceOperation() throws Exception {
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
    void testAggregateWithMatchProjectReduceConcatAndCond() throws Exception {
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
    void testAggregateWithBucketStage() throws Exception {
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
    void testAggregateWithIllegalBucketStage() throws Exception {
        collection.insertOne(json("_id: 1"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$bucket: {groupBy: '$_id', boundaries: [100, 200, 400]}")).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 40066 (Location40066): 'PlanExecutor error during aggregation :: caused by :: $switch could not find a matching branch for an input, and no default was specified.'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$bucket: {groupBy: '$_id', boundaries: [0, 400], default: 200}")).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 40199 (Location40199): 'The $bucket 'default' field must be less than the lowest boundary or greater than or equal to the highest boundary.'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$bucket: {groupBy: '$_id', boundaries: [0, 400, 200]}")).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 40194 (Location40194): 'The 'boundaries' option to $bucket must be sorted, but elements 1 and 2 are not in ascending order (400 is not less than 200).'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$bucket: {groupBy: '$_id', boundaries: [0, 400], output: 'a'}")).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 40196 (Location40196): 'The $bucket 'output' field must be an object, but found type: string.'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$bucket: {groupBy: '$_id', boundaries: [0]}")).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 40192 (Location40192): 'The $bucket 'boundaries' field must have at least 2 values, but found 1 value(s).'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$bucket: {groupBy: '$_id', boundaries: 'abc'}")).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 40200 (Location40200): 'The $bucket 'boundaries' field must be an array, but found type: string.");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$bucket: {groupBy: [1, 2], boundaries: 'abc'}")).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 40202 (Location40202): 'The $bucket 'groupBy' field must be defined as a $-prefixed path or an expression, but found: [ 1, 2 ].'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$bucket: {groupBy: '$_id'}")).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 40198 (Location40198): '$bucket requires 'groupBy' and 'boundaries' to be specified.'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$bucket: {}")).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 40198 (Location40198): '$bucket requires 'groupBy' and 'boundaries' to be specified.'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$bucket: {groupBy: '$_id', boundaries: ['abc', 123]}")).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 40193 (Location40193): 'All values in the the 'boundaries' option to $bucket must have the same type. Found conflicting types string and int.'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$bucket: {groupBy: '$_id', boundaries: [0, null]}")).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 40193 (Location40193): 'All values in the the 'boundaries' option to $bucket must have the same type. Found conflicting types int and null.'");
    }

    @Test
    void testAggregateWithFacetStage() throws Exception {
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
    void testAggregateWithMatchAndFacetStage() throws Exception {
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
    void testAggregateWithUnsetStage() throws Exception {
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
    void testAggregateWithUnsetStage_illegalInput() throws Exception {
        collection.insertOne(json("_id: 1"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$unset: [123]")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 31120 (Location31120): '$unset specification must be a string or an array containing only string values'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$unset: ['']")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 40352 (Location40352): 'Invalid $unset :: caused by :: FieldPath cannot be constructed with empty string'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$unset: ['field1', 123]")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 31120 (Location31120): '$unset specification must be a string or an array containing only string values'");
    }

    @Test
    void testAggregateWithIndexStats() throws Exception {
        List<Document> pipeline = jsonList("$indexStats: {}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));

        Document indexStats = CollectionUtils.getSingleElement(collection.aggregate(pipeline));
        assertThat(indexStats)
            .containsOnlyKeys("name", "key", "host", "accesses", "spec")
            .containsEntry("name", "_id_")
            .containsEntry("key", json("_id: 1"))
            .containsEntry("spec", json("key: {_id: 1}, name: '_id_', v: 2"));

        assertThat((Document) indexStats.get("accesses"))
            .containsEntry("ops", 0L);
    }

    @Test
    void testAggregateWithOut() {
        List<Document> pipeline = jsonList(
            "$group: {_id: '$author', books: {$push: '$title'}}",
            "$out : 'authors'");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 8751, title: 'The Banquet', author: 'Dante', copies: 2"));
        collection.insertOne(json("_id: 8752, title: 'Divine Comedy', author: 'Dante', copies: 1"));
        collection.insertOne(json("_id: 8645, title: 'Eclogues', author: 'Dante', copies: 2"));
        collection.insertOne(json("_id: 7000, title: 'The Odyssey', author: 'Homer', copies: 10"));
        collection.insertOne(json("_id: 7020, title: 'Iliad', author: 'Homer', copies: 10"));

        List<Document> expectedDocuments = List.of(
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
    void testAggregateWithOut_illegal() {
        collection.insertOne(json("_id: 8751, title: 'The Banquet', author: 'Dante', copies: 2"));

        assertThatExceptionOfType(IllegalStateException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$out : 123")).first())
            .withMessage("Cannot return a cursor when the value for $out stage is not a string or namespace document");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$out : ''")).first())
            .withMessage("state should be: collectionName is not empty");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$out : 'some$collection'")).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 20 (IllegalOperation): 'PlanExecutor error during aggregation :: caused by :: error with target namespace: Invalid collection name: some$collection'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$out : 'one'", "$out : 'other'")).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 40601 (Location40601): '$out can only be the final stage in the pipeline'");
    }

    // Testing the official example from https://www.mongodb.com/docs/manual/reference/operator/aggregation/merge/ (2022-11-22)
    @Test
    void testAggregateWithMerge_officialExample() throws Exception {
        syncClient.getDatabase("reporting").drop();

        collection.insertMany(jsonList(
            "_id: 1, employee: 'Ant', dept: 'A', salary: 100000, fiscal_year: 2017",
            "_id: 2, employee: 'Bee', dept: 'A', salary: 120000, fiscal_year: 2017",
            "_id: 3, employee: 'Cat', dept: 'Z', salary: 115000, fiscal_year: 2017",
            "_id: 4, employee: 'Ant', dept: 'A', salary: 115000, fiscal_year: 2018",
            "_id: 5, employee: 'Bee', dept: 'Z', salary: 145000, fiscal_year: 2018",
            "_id: 6, employee: 'Cat', dept: 'Z', salary: 135000, fiscal_year: 2018",
            "_id: 7, employee: 'Gecko', dept: 'A', salary: 100000, fiscal_year: 2018",
            "_id: 8, employee: 'Ant', dept: 'A', salary: 125000, fiscal_year: 2019",
            "_id: 9, employee: 'Bee', dept: 'Z', salary: 160000, fiscal_year: 2019",
            "_id: 10, employee: 'Cat', dept: 'Z', salary: 150000, fiscal_year: 2019"
        ));

        assertThat(collection.aggregate(jsonList(
            "$group: { _id: { fiscal_year: '$fiscal_year', dept: '$dept' }, salaries: { $sum: '$salary' }}",
            "$sort: { '_id.fiscal_year': -1, '_id.dept': 1 }",
            "$merge: { into: { db: 'reporting', coll: 'budgets' }, on: '_id',  whenMatched: 'replace', whenNotMatched: 'insert' }"
        ))).containsExactlyElementsOf(jsonList(
            "_id: {dept: 'A', fiscal_year: 2019}, salaries: 125000",
            "_id: {dept: 'Z', fiscal_year: 2019}, salaries: 310000",
            "_id: {dept: 'A', fiscal_year: 2018}, salaries: 215000",
            "_id: {dept: 'Z', fiscal_year: 2018}, salaries: 280000",
            "_id: {dept: 'A', fiscal_year: 2017}, salaries: 220000",
            "_id: {dept: 'Z', fiscal_year: 2017}, salaries: 115000"
        ));

        MongoDatabase reportingDb = syncClient.getDatabase("reporting");
        assertThat(reportingDb.listCollectionNames()).containsExactly("budgets");

        MongoCollection<Document> budgets = reportingDb.getCollection("budgets");
        assertThat(budgets.find().sort(json("salaries: 1")))
            .containsExactlyElementsOf(jsonList(
                "_id: {dept: 'Z', fiscal_year: 2017}, salaries: 115000",
                "_id: {dept: 'A', fiscal_year: 2019}, salaries: 125000",
                "_id: {dept: 'A', fiscal_year: 2018}, salaries: 215000",
                "_id: {dept: 'A', fiscal_year: 2017}, salaries: 220000",
                "_id: {dept: 'Z', fiscal_year: 2018}, salaries: 280000",
                "_id: {dept: 'Z', fiscal_year: 2019}, salaries: 310000"
            ));
    }

    @Test
    void testAggregateWithMerge_otherCollection() throws Exception {
        collection.insertMany(jsonList(
            "_id: 1, employee: 'Ant'",
            "_id: 2, employee: 'Bee'",
            "_id: 3, employee: 'Cat'"
        ));

        assertThat(collection.aggregate(jsonList("$merge: { into: 'other' }")))
            .containsExactlyInAnyOrderElementsOf(jsonList(
                "_id: 1, employee: 'Ant'",
                "_id: 2, employee: 'Bee'",
                "_id: 3, employee: 'Cat'"
            ));

        assertThat(db.getCollection("other").find().sort(json("_id: 1")))
            .containsExactlyElementsOf(jsonList(
                "_id: 1, employee: 'Ant'",
                "_id: 2, employee: 'Bee'",
                "_id: 3, employee: 'Cat'"
            ));
    }

    @Test
    void testAggregateWithMerge_sameCollection() throws Exception {
        collection.insertMany(jsonList(
            "_id: 1, employee: 'Ant', otherId: 4",
            "_id: 2, employee: 'Bee', otherId: 5",
            "_id: 3, employee: 'Cat', otherId: 6"
        ));

        assertThat(collection.aggregate(jsonList(
            "$project: { _id: '$otherId', employee: 1 }",
            "$merge: { into: '" + collection.getNamespace().getCollectionName() + "' }"
        ))).containsExactlyInAnyOrderElementsOf(jsonList(
            "_id: 1, employee: 'Ant', otherId: 4",
            "_id: 2, employee: 'Bee', otherId: 5",
            "_id: 3, employee: 'Cat', otherId: 6",
            "_id: 4, employee: 'Ant'",
            "_id: 5, employee: 'Bee'",
            "_id: 6, employee: 'Cat'"
        ));

        assertThat(collection.find().sort(json("_id: 1")))
            .containsExactlyElementsOf(jsonList(
                "_id: 1, employee: 'Ant', otherId: 4",
                "_id: 2, employee: 'Bee', otherId: 5",
                "_id: 3, employee: 'Cat', otherId: 6",
                "_id: 4, employee: 'Ant'",
                "_id: 5, employee: 'Bee'",
                "_id: 6, employee: 'Cat'"
            ));
    }

    @Test
    void testAggregateWithMerge_mergeDocuments_simple() throws Exception {
        collection.insertMany(jsonList(
            "_id: 1, employee: 'Ant'",
            "_id: 2, employee: 'Bee'",
            "_id: 3, employee: 'Cat'"
        ));

        assertThat(collection.aggregate(jsonList(
            "$addFields: { newField: 1 }",
            "$merge: { into: '" + collection.getNamespace().getCollectionName() + "', on: '_id', whenMatched: 'merge', whenNotMatched: 'fail' }"
        ))).containsExactlyInAnyOrderElementsOf(jsonList(
            "_id: 1, employee: 'Ant', newField: 1",
            "_id: 2, employee: 'Bee', newField: 1",
            "_id: 3, employee: 'Cat', newField: 1"));

        assertThat(collection.find().sort(json("_id: 1")))
            .containsExactlyElementsOf(jsonList(
                "_id: 1, employee: 'Ant', newField: 1",
                "_id: 2, employee: 'Bee', newField: 1",
                "_id: 3, employee: 'Cat', newField: 1"
            ));
    }

    @Test
    void testAggregateWithMerge_mergeDocuments_complex() throws Exception {
        collection.insertMany(jsonList(
            "_id: 1, x: 1",
            "_id: 2, x: { a: 1, b: 2 }",
            "_id: 3, x: { a: 1, b: 2 }"
        ));

        MongoCollection<Document> otherCollection = db.getCollection("other");

        otherCollection.insertMany(jsonList(
            "_id: 1, x: 2",
            "_id: 2, x: 'abc'",
            "_id: 3, x: { c: 4 }"
        ));

        assertThat(collection.aggregate(jsonList("$merge: { into: 'other' }")))
            .containsExactlyInAnyOrderElementsOf(jsonList(
                "_id: 1, x: 1",
                "_id: 2, x: { a: 1, b: 2}",
                "_id: 3, x: { a: 1, b: 2}"
            ));

        assertThat(otherCollection.find().sort(json("_id: 1")))
            .containsExactlyElementsOf(jsonList(
                "_id: 1, x: 1",
                "_id: 2, x: { a: 1, b: 2}",
                "_id: 3, x: { a: 1, b: 2}"
            ));
    }

    @Test
    void testAggregateWithMerge_replaceDocuments() throws Exception {
        collection.insertMany(jsonList(
            "_id: 1, employee: 'Ant'",
            "_id: 2, employee: 'Bee'",
            "_id: 3, employee: 'Cat'"
        ));

        assertThat(collection.aggregate(jsonList(
            "$addFields: { newField: 1 }",
            "$project: { employee: 0 }",
            "$merge: { into: '" + collection.getNamespace().getCollectionName() + "', on: '_id', whenMatched: 'replace', whenNotMatched: 'fail' }"
        ))).containsExactlyInAnyOrderElementsOf(jsonList(
            "_id: 1, newField: 1",
            "_id: 2, newField: 1",
            "_id: 3, newField: 1"
        ));

        assertThat(collection.find().sort(json("_id: 1")))
            .containsExactlyElementsOf(jsonList(
                "_id: 1, newField: 1",
                "_id: 2, newField: 1",
                "_id: 3, newField: 1"
            ));
    }

    @Test
    void testAggregateWithMerge_keepExistingDocuments() throws Exception {
        collection.insertMany(jsonList(
            "_id: 1, employee: 'Ant'",
            "_id: 2, employee: 'Bee'",
            "_id: 3, employee: 'Cat'"
        ));

        assertThat(collection.aggregate(jsonList(
            "$addFields: { newField: 1 }",
            "$merge: { into: '" + collection.getNamespace().getCollectionName() + "', on: '_id', whenMatched: 'keepExisting' }"
        ))).containsExactlyInAnyOrderElementsOf(jsonList(
            "_id: 1, employee: 'Ant'",
            "_id: 2, employee: 'Bee'",
            "_id: 3, employee: 'Cat'"
        ));

        assertThat(collection.find().sort(json("_id: 1")))
            .containsExactlyElementsOf(jsonList(
                "_id: 1, employee: 'Ant'",
                "_id: 2, employee: 'Bee'",
                "_id: 3, employee: 'Cat'"
            ));
    }

    @Test
    void testAggregateWithMerge_whenNotMatched_discard() throws Exception {
        collection.insertMany(jsonList(
            "_id: 1, employee: 'Ant'",
            "_id: 2, employee: 'Bee'",
            "_id: 3, employee: 'Cat'"
        ));

        MongoCollection<Document> otherCollection = db.getCollection("other");
        otherCollection.insertMany(jsonList(
            "_id: 1",
            "_id: 3"
        ));

        assertThat(collection.aggregate(jsonList("$merge: { into: 'other', whenNotMatched: 'discard' }")))
            .containsExactlyInAnyOrderElementsOf(jsonList(
                "_id: 1, employee: 'Ant'",
                "_id: 3, employee: 'Cat'"
            ));

        assertThat(otherCollection.find().sort(json("_id: 1")))
            .containsExactlyElementsOf(jsonList(
                "_id: 1, employee: 'Ant'",
                "_id: 3, employee: 'Cat'"
            ));
    }

    @Test
    void testAggregateWithMerge_whenMatched_fail() throws Exception {
        collection.insertMany(jsonList(
            "_id: 1, employee: 'Ant'",
            "_id: 2, employee: 'Bee'",
            "_id: 3, employee: 'Cat'"
        ));

        MongoCollection<Document> otherCollection = db.getCollection("other");
        otherCollection.insertMany(jsonList(
            "_id: 1",
            "_id: 3"
        ));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$merge: { into: 'other', whenMatched: 'fail' }")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 11000 (DuplicateKey): " +
                "'PlanExecutor error during aggregation :: caused by :: " +
                "E11000 duplicate key error collection: testdb.other index: _id_ dup key: { _id: 1 }'");
    }

    @Test
    void testAggregateWithMerge_whenNotMatched_fail() throws Exception {
        collection.insertMany(jsonList(
            "_id: 1, employee: 'Ant'",
            "_id: 2, employee: 'Bee'",
            "_id: 3, employee: 'Cat'"
        ));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$merge: { into: 'other', whenNotMatched: 'fail' }")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 13113 (MergeStageNoMatchingDocument): " +
                "'PlanExecutor error during aggregation :: caused by :: " +
                "$merge could not find a matching document in the target collection for at least one document in the source collection'");
    }


    @Test
    void testAggregateWithMerge_multipleOnFields() throws Exception {
        collection.insertMany(jsonList(
            "_id: 1, name: 'Ant', year: 2020, data: 'a'",
            "_id: 2, name: 'Bee', year: 2021, data: 'b'",
            "_id: 3, name: 'Cat', year: 2022, data: 'c'",
            "_id: 4, name: 'Ant', year: 2021, data: 'd'"
        ));

        MongoCollection<Document> otherCollection = db.getCollection("other");

        otherCollection.insertMany(jsonList(
            "_id: 10, name: 'Ant', year: 2020",
            "_id: 11, name: 'Bee', year: 2021",
            "_id: 12, name: 'Cat', year: 2022",
            "_id: 13, name: 'Ant', year: 2021"
        ));

        otherCollection.createIndex(json("year: 1, name: -1"), new IndexOptions().unique(true));

        assertThat(collection.aggregate(jsonList(
            "$project: { _id: 0 }",
            "$merge: { into: 'other', on: ['name', 'year'] }"
        ))).containsExactlyInAnyOrderElementsOf(jsonList(
            "_id: 10, name: 'Ant', year: 2020, data: 'a'",
            "_id: 11, name: 'Bee', year: 2021, data: 'b'",
            "_id: 12, name: 'Cat', year: 2022, data: 'c'",
            "_id: 13, name: 'Ant', year: 2021, data: 'd'"
        ));

        assertThat(otherCollection.find().sort(json("_id: 1")))
            .containsExactlyElementsOf(jsonList(
                "_id: 10, name: 'Ant', year: 2020, data: 'a'",
                "_id: 11, name: 'Bee', year: 2021, data: 'b'",
                "_id: 12, name: 'Cat', year: 2022, data: 'c'",
                "_id: 13, name: 'Ant', year: 2021, data: 'd'"
            ));
    }

    @Test
    void testAggregateWithMerge_stringParameter() throws Exception {
        collection.insertMany(jsonList(
            "_id: 1, name: 'Ant', year: 2020, data: 'a'",
            "_id: 2, name: 'Bee', year: 2021, data: 'b'",
            "_id: 3, name: 'Cat', year: 2022, data: 'c'"
        ));

        assertThat(collection.aggregate(jsonList(
            "$project: { year: 0, data: 0 }",
            "$merge: 'other'"
        ))).containsExactlyInAnyOrderElementsOf(jsonList(
            "_id: 1, name: 'Ant'",
            "_id: 2, name: 'Bee'",
            "_id: 3, name: 'Cat'"));

        assertThat(db.getCollection("other").find().sort(json("_id: 1")))
            .containsExactlyElementsOf(jsonList(
                "_id: 1, name: 'Ant'",
                "_id: 2, name: 'Bee'",
                "_id: 3, name: 'Cat'"
            ));
    }

    @Test
    void testAggregateWithMerge_idMustNotBeModified_merge() throws Exception {
        collection.insertMany(jsonList(
            "_id: 1, name: 'Ant'",
            "_id: 2, name: 'Bee'",
            "_id: 3, name: 'Cat'"
        ));

        MongoCollection<Document> otherCollection = db.getCollection("other");

        otherCollection.insertMany(jsonList(
            "_id: 10, name: 'Ant'",
            "_id: 11, name: 'Bee'",
            "_id: 12, name: 'Cat'"
        ));

        otherCollection.createIndex(json("name: 1"), new IndexOptions().unique(true));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$merge: { into: 'other', on: 'name' }")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 66 (ImmutableField): " +
                "'PlanExecutor error during aggregation :: caused by :: " +
                "$merge failed to update the matching document, did you attempt to modify the _id or the shard key? :: caused by :: " +
                "Performing an update on the path '_id' would modify the immutable field '_id''");
    }

    @Test
    void testAggregateWithMerge_idMustNotBeModified_replace() throws Exception {
        collection.insertMany(jsonList(
            "_id: 1, name: 'Ant'",
            "_id: 2, name: 'Bee'",
            "_id: 3, name: 'Cat'"
        ));

        MongoCollection<Document> otherCollection = db.getCollection("other");

        otherCollection.insertMany(jsonList(
            "_id: 10, name: 'Ant'",
            "_id: 11, name: 'Bee'",
            "_id: 12, name: 'Cat'"
        ));

        otherCollection.createIndex(json("name: 1"), new IndexOptions().unique(true));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$merge: { into: 'other', on: 'name', whenMatched: 'replace' }")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 66 (ImmutableField): " +
                "'PlanExecutor error during aggregation :: caused by :: " +
                "$merge failed to update the matching document, did you attempt to modify the _id or the shard key? :: caused by :: " +
                "After applying the update, the (immutable) field '_id' was found to have been altered to _id: 1'");
    }

    @Test
    void testAggregateWithMerge_onWithoutUniqueIndex() throws Exception {
        collection.insertOne(json("_id: 1"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$merge: { into: 'other', on: 'xyz' }")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 51183 (Location51183): " +
                "'Cannot find index to verify that join fields will be unique'");
    }

    @Test
    void testAggregateWithMerge_onWithIndexWhichIsNotUnique() throws Exception {
        collection.insertOne(json("_id: 1"));
        collection.createIndex(json("xyz: 1"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$merge: { into: '" + collection.getNamespace().getCollectionName() + "', on: 'xyz' }")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 51183 (Location51183): " +
                "'Cannot find index to verify that join fields will be unique'");
    }

    @Test
    void testAggregateWithMerge_pipeline() throws Exception {
        collection.insertOne(json("_id: 1"));

        MongoCollection<Document> otherCollection = db.getCollection("other");
        otherCollection.insertOne(json("_id: 1, x: 123"));

        assertThat(collection.aggregate(
            jsonList("$merge: {into: 'other', let: {}, whenMatched: []}")))
            .containsExactly(json("_id: 1, x: 123"));

        assertThat(collection.aggregate(
            jsonList("$merge: {into: 'other', whenMatched: [{$addFields: {test: 123}}]}")))
            .containsExactly(json("_id: 1, x: 123, test: 123"));

        assertThat(collection.aggregate(
            jsonList("$merge: {into: 'other', let: {year: 2020}, whenMatched: [{$project: {test: 0}}, {$addFields: {salesYear: '$$year'}}, {$unset: 'x'}]}")))
            .containsExactly(json("_id: 1, salesYear: 2020"));

        assertThat(otherCollection.find())
            .containsExactly(json("_id: 1, salesYear: 2020"));
    }

    @Test
    void testAggregateWithMerge_pipeline_newVariable() throws Exception {
        collection.insertOne(json("_id: 1, obj: {a: 1, b: 'xyz'}"));

        MongoCollection<Document> otherCollection = db.getCollection("other");
        otherCollection.insertOne(json("_id: 1"));

        assertThat(collection.aggregate(
            jsonList("$merge: {into: 'other', whenMatched: [{$addFields: {b: '$$new.obj.b'}}]}")))
            .containsExactly(json("_id: 1, b: 'xyz'"));

        assertThat(otherCollection.find())
            .containsExactly(json("_id: 1, b: 'xyz'"));

        assertThat(collection.aggregate(
            jsonList("$merge: {into: 'other', let: {new: '$$ROOT'}, whenMatched: [{$addFields: {a: '$$new.obj.a'}}]}")))
            .containsExactly(json("_id: 1, b: 'xyz', a: 1"));

        assertThat(otherCollection.find())
            .containsExactly(json("_id: 1, b: 'xyz', a: 1"));
    }

    @Test
    void testAggregateWithMerge_pipeline_nowVariable() throws Exception {
        collection.insertOne(json("_id: 1"));

        MongoCollection<Document> otherCollection = db.getCollection("other");
        otherCollection.insertOne(json("_id: 1"));

        Instant instantBefore = Instant.now();
        Document result = collection.aggregate(
            jsonList("$merge: {into: 'other', let: {now: '$$NOW'}, whenMatched: [{$addFields: {timestamp: '$$now'}}]}")).first();
        Instant instantAfter = Instant.now();

        assertThat(result).containsOnlyKeys("_id", "timestamp");
        System.out.println(result.get("timestamp"));
        assertThat(result.getDate("timestamp")).isBetween(instantBefore, instantAfter);
    }

    private static Stream<Arguments> aggregateWithMerge_illegalParametersArguments() {
        return Stream.of(
            Arguments.of("$merge: null", IllegalStateException.class,
                "Cannot return a cursor when the value for $merge stage is not a string or a document"),

            Arguments.of("$merge: []", IllegalStateException.class,
                "Cannot return a cursor when the value for $merge stage is not a string or a document"),

            Arguments.of("$merge: 1", IllegalStateException.class,
                "Cannot return a cursor when the value for $merge stage is not a string or a document"),

            Arguments.of("$merge: { otherParam: 1 }", MongoCommandException.class,
                "Command execution failed on MongoDB server with error 40415 (Location40415): 'BSON field '$merge.otherParam' is an unknown field.'"),

            Arguments.of("$merge: { into: {} }", BsonInvalidOperationException.class,
                "Document does not contain key coll"),

            Arguments.of("$merge: { into: { coll: 'abc', other: 1} }", MongoCommandException.class,
                "Command execution failed on MongoDB server with error 40415 (Location40415): 'BSON field 'into.other' is an unknown field.'"),

            Arguments.of("$merge: { into: { coll: 1} }", BsonInvalidOperationException.class,
                "Value expected to be of type STRING is of unexpected type INT32"),

            Arguments.of("$merge: { into: { db: 1, coll: 'xyz' } }", BsonInvalidOperationException.class,
                "Value expected to be of type STRING is of unexpected type INT32"),

            Arguments.of("$merge: { into: 'abc', on: 1 }", MongoCommandException.class,
                "Command execution failed on MongoDB server with error 51186 (Location51186): '$merge 'on' field  must be either a string or an array of strings, but found int'"),

            Arguments.of("$merge: { into: 'abc', on: [1, 2, 3] }", MongoCommandException.class,
                "Command execution failed on MongoDB server with error 51134 (Location51134): '$merge 'on' array elements must be strings, but found int'"),

            Arguments.of("$merge: { into: 'abc', let: 1 }", MongoCommandException.class,
                "Command execution failed on MongoDB server with error 14 (TypeMismatch): 'BSON field '$merge.let' is the wrong type 'int', expected type 'object''"),

            Arguments.of("$merge: { into: 'abc', let: [] }", MongoCommandException.class,
                "Command execution failed on MongoDB server with error 14 (TypeMismatch): 'BSON field '$merge.let' is the wrong type 'array', expected type 'object''"),

            Arguments.of("$merge: { into: 'abc', let: {} }", MongoCommandException.class,
                "Command execution failed on MongoDB server with error 51199 (Location51199): 'Cannot use 'let' variables with 'whenMatched: merge' mode'"),

            Arguments.of("$merge: { into: 'abc', let: {new: 1}, whenMatched: [] }", MongoCommandException.class,
                "Command execution failed on MongoDB server with error 51273 (Location51273): ''let' may not define a value for the reserved 'new' variable other than '$$ROOT''"),

            Arguments.of("$merge: { into: 'abc', on: ['a', 'b', 'a'] }", MongoCommandException.class,
                "Command execution failed on MongoDB server with error 31465 (Location31465): 'Found a duplicate field 'a''"),

            Arguments.of("$merge: { into: 'abc', on: [] }", MongoCommandException.class,
                "Command execution failed on MongoDB server with error 51187 (Location51187): 'If explicitly specifying $merge 'on', must include at least one field'"),

            Arguments.of("$merge: { into: 'abc', whenMatched: 1 }", MongoCommandException.class,
                "Command execution failed on MongoDB server with error 51191 (Location51191): '$merge 'whenMatched' field  must be either a string or an array, but found int'"),

            Arguments.of("$merge: { into: 'abc', whenMatched: 'other' }", MongoCommandException.class,
                "Command execution failed on MongoDB server with error 2 (BadValue): 'Enumeration value 'other' for field 'whenMatched' is not a valid value.'"),

            Arguments.of("$merge: { into: 'abc', whenMatched: ['a', 'b'] }", MongoCommandException.class,
                "Command execution failed on MongoDB server with error 14 (TypeMismatch): 'Each element of the 'pipeline' array must be an object'"),

            Arguments.of("$merge: { into: 'abc', whenMatched: [{$sort: {a: 1}}] }", MongoCommandException.class,
                "Command execution failed on MongoDB server with error 72 (InvalidOptions): 'PlanExecutor error during aggregation :: caused by :: $sort is not allowed to be used within an update'"),

            Arguments.of("$merge: { into: 'abc', whenNotMatched: 1 }", MongoCommandException.class,
                "Command execution failed on MongoDB server with error 14 (TypeMismatch): 'BSON field '$merge.whenNotMatched' is the wrong type 'int', expected type 'string''"),

            Arguments.of("$merge: { into: 'abc', whenNotMatched: 'other' }", MongoCommandException.class,
                "Command execution failed on MongoDB server with error 2 (BadValue): 'Enumeration value 'other' for field '$merge.whenNotMatched' is not a valid value.'"),

            Arguments.of("$merge: { into: 1 }", MongoCommandException.class,
                "Command execution failed on MongoDB server with error 51178 (Location51178): '$merge 'into' field  must be either a string or an object, but found int'")
        );
    }

    @ParameterizedTest
    @MethodSource("aggregateWithMerge_illegalParametersArguments")
    void testAggregateWithMerge_illegalParameters(String merge, Class<? extends Exception> expectedException, String expectedMessage) throws Exception {
        collection.insertOne(json("_id: 1"));

        assertThatExceptionOfType(expectedException)
            .isThrownBy(() -> collection.aggregate(jsonList(merge)).first())
            .withMessageStartingWith(expectedMessage);
    }

    @Test
    void testAggregateWithMerge_mergeIsNotTheLastStage() throws Exception {
        collection.insertOne(json("_id: 1"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList(
                "$merge: { into: 'other' }",
                "$project : { _id: 0 }"
            )).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 40601 (Location40601): " +
                "'$merge can only be the final stage in the pipeline'");
    }

    @Test
    void testAggregateWithRandInMatch() {
        List<Document> pipeline = jsonList("$match: { $expr: { $lt: [0.5, {$rand: {} }]}}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3"));
        collection.insertOne(json("_id: 4"));

        for (int i = 0; i < 10; i++) {
            if (toArray(collection.aggregate(pipeline)).size() == 2) {
                return;
            }
        }
        fail("Expecting to receive 50% of all documents in at least one of 10 trials");
    }

    @Test
    void testAggregateWithRandInProjection() {
        List<Document> pipeline = jsonList("$project: { _id: 1, rand: {$rand: {}}}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3"));
        collection.insertOne(json("_id: 4"));

        Set<Double> values = new LinkedHashSet<>();
        for (Document document : toArray(collection.aggregate(pipeline))) {
            assertThat(document.get("rand")).isInstanceOf(Double.class);
            Double rand = document.getDouble("rand");
            assertThat(rand).isBetween(0.0, 1.0);
            values.add(rand);
        }
        assertThat(values).hasSize(4);
    }

    @Test
    void testAggregateWithRand_illegalArguments() {
        collection.insertOne(json("_id: 1"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: { r: {$rand: {a: 1}}}")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 3040501 (Location3040501): " +
                "'Invalid $project :: caused by :: $rand does not currently accept arguments'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: { r: {$rand: [{}, {}]}}")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 3040501 (Location3040501): " +
                "'Invalid $project :: caused by :: $rand does not currently accept arguments'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$project: { r: {$rand: null}}")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 10065 (Location10065): " +
                "'Invalid $project :: caused by :: invalid parameter: expected an object ($rand)'");

        assertThat(collection.aggregate(jsonList("$project: { r: {$rand: []}}")))
            .map(Document::toJson)
            .hasSize(1)
            .allMatch(document -> document.matches("\\{\"_id\": 1, \"r\": 0\\.\\d+}"));
    }

    @Test
    void testAggregateWithRedact() {
        List<Document> pipeline = jsonList("$redact: {$cond: {if: {$eq: [\"$_id\", 1]}, then: \"$$KEEP\", else: \"$$PRUNE\"}}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));

        assertThat(collection.aggregate(pipeline))
            .containsOnly(json("_id: 1"));
    }

    @Test
    void dotNotationProjection() {
        collection.insertOne(json("""
            _id: 1,
            companyName: 'Walt Disney',
            buildingNumber: 500,
            streetName: 'South Buena Vista Street',
            city: 'Burbank',
            state: 'California',
            zipCode: 91502
            """));

        assertThat(collection.aggregate(jsonList("""
            $project: {
                Name: "$companyName",
                "Address.City": "$city",
                "Address.ZipCode": "$zipCode",
                "Address.StreetName": "$streetName",
                "Address.BuildingNumber": "$buildingNumber",
            }
            """)))
            .containsExactly(json("""
                _id: 1,
                Name: 'Walt Disney',
                Address: {
                   City: 'Burbank',
                   ZipCode: 91502,
                   StreetName: 'South Buena Vista Street',
                   BuildingNumber: 500
                }
                """));
    }

    @Test
    void dotNotationProjectionDeepNesting() {
        collection.insertOne(json("""
            _id: 1,
            companyName: 'Acme Corp',
            country: 'USA',
            state: 'California',
            city: 'San Francisco',
            street: 'Market Street',
            buildingNumber: 123,
            floor: 5,
            unit: 'A'
            """));

        assertThat(collection.aggregate(jsonList("""
            $project: {
                Company: "$companyName",
                "Location.Country": "$country",
                "Location.Address.State": "$state",
                "Location.Address.City": "$city",
                "Location.Address.Details.Street": "$street",
                "Location.Address.Details.BuildingNumber": "$buildingNumber",
                "Location.Address.Details.Floor": "$floor",
                "Location.Address.Details.Unit": "$unit"
            }
            """)))
            .containsExactly(json("""
                _id: 1,
                Company: 'Acme Corp',
                Location: {
                   Country: 'USA',
                   Address: {
                      State: 'California',
                      City: 'San Francisco',
                      Details: {
                         Street: 'Market Street',
                         BuildingNumber: 123,
                         Floor: 5,
                         Unit: 'A'
                      }
                   }
                }
                """));
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/191
    @Test
    void testProjectWithCondition() throws Exception {
        List<Document> pipeline = jsonList("$project: {numItems: {$cond : [{$isArray : '$item'}, {$size: '$item'}, 0]}}");

        assertThat(collection.aggregate(pipeline)).isEmpty();

        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2, item: 'abc'"));
        collection.insertOne(json("_id: 3, item: [1, 2, 3]"));

        assertThat(collection.aggregate(pipeline))
            .containsOnly(
                json("_id: 1, numItems: 0"),
                json("_id: 2, numItems: 0"),
                json("_id: 3, numItems: 3")
            );
    }

    @Test
    void testAggregateWithSwitch() throws Exception {
        collection.insertOne(json("_id: 1, name: 'Dave', qty: 1"));
        collection.insertOne(json("_id: 2, name: 'Carol', qty: 5"));
        collection.insertOne(json("_id: 3, name: 'Bob', qty: 10"));
        collection.insertOne(json("_id: 4, name: 'Alice', qty: 20"));

        List<Document> pipeline = jsonList("""
            $project: {
              name: 1,
              qtyDiscount: {
                $switch: {
                  branches: [
                    { case: { $gte: ['$qty', 10] }, then: 0.15 },
                    { case: { $gte: ['$qty', 5] }, then: 0.10 },
                    { case: { $gte: ['$qty', 1] }, then: 0.05 }
                  ],
                  default: 0
                }
              }
            }
            """);

        assertThat(collection.aggregate(pipeline))
            .containsExactlyInAnyOrder(
                json("_id: 1, name: 'Dave', qtyDiscount: 0.05"),
                json("_id: 2, name: 'Carol', qtyDiscount: 0.10"),
                json("_id: 3, name: 'Bob', qtyDiscount: 0.15"),
                json("_id: 4, name: 'Alice', qtyDiscount: 0.15")
            );
    }

    @Test
    void testAggregateWithSwitchDefault() throws Exception {
        collection.insertOne(json("_id: 1, status: 'active'"));
        collection.insertOne(json("_id: 2, status: 'inactive'"));
        collection.insertOne(json("_id: 3, status: 'unknown'"));

        List<Document> pipeline = jsonList("""
            $project: {
              statusCode: {
                $switch: {
                  branches: [
                    { case: { $eq: ['$status', 'active'] }, then: 1 },
                    { case: { $eq: ['$status', 'inactive'] }, then: 0 }
                  ],
                  default: -1
                }
              }
            }
            """);

        assertThat(collection.aggregate(pipeline))
            .containsExactlyInAnyOrder(
                json("_id: 1, statusCode: 1"),
                json("_id: 2, statusCode: 0"),
                json("_id: 3, statusCode: -1")
            );
    }

    @Test
    void testAggregateWithSwitchMissingDefault() throws Exception {
        collection.insertOne(json("_id: 1, value: 100"));

        List<Document> pipeline = jsonList("""
            $project: {
              result: {
                $switch: {
                  branches: [
                    { case: { $eq: ['$value', 50] }, then: 'fifty' }
                  ]
                }
              }
            }
            """);

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("$switch could not find a matching branch for an input, and no default was specified");
    }

    @Test
    void testAggregateWithSwitchMissingBranches() throws Exception {
        collection.insertOne(json("_id: 1, value: 100"));

        List<Document> pipeline = jsonList("""
            $project: {
              result: {
                $switch: {
                  default: 'none'
                }
              }
            }
            """);

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("$switch requires at least one branch");
    }

    @Test
    void testAggregateWithSwitchEmptyBranches() throws Exception {
        collection.insertOne(json("_id: 1, value: 100"));

        List<Document> pipeline = jsonList("""
            $project: {
              result: {
                $switch: {
                  branches: [
                  ],
                  default: 'none'
                }
              }
            }
            """);

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("$switch requires at least one branch");
    }

    @Test
    void testAggregateWithSwitchInvalidBranch() throws Exception {
        collection.insertOne(json("_id: 1, value: 100"));

        List<Document> pipeline = jsonList("""
            $project: {
              result: {
                $switch: {
                  branches: [
                    { case: { $eq: ['$value', 100] } }
                  ],
                  default: 'none'
                }
              }
            }
            """);

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("$switch requires each branch have a 'then' expression");
    }

    @Test
    void testAggregateWithSwitchInvalidArgument() throws Exception {
        collection.insertOne(json("_id: 1, value: 100"));

        List<Document> pipeline = jsonList("""
            $project: {
              result: {
                $switch: {
                  branches: [
                    { case: { $eq: ['$value', 100] }, then: 'one hundred' }
                  ],
                  default_value: 'none'
                }
              }
            }
            """);

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("$switch found an unknown argument: default_value");
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/138
    @Test
    public void testAggregateWithGeoNear() throws Exception {
        List<Document> pipeline = jsonList("$geoNear: {}");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("Command execution failed on MongoDB server with error -1: '$geoNear is not yet implemented. See https://github.com/bwaldvogel/mongo-java-server/issues/138'");
    }

    private static Stream<Arguments> aggregateWithToDoubleArguments() {
        return Stream.of(
            Arguments.of("12", 12.0),
            Arguments.of("7.5", 7.5),
            Arguments.of(9, 9.0),
            Arguments.of(9.5, 9.5),
            Arguments.of(false, 0.0),
            Arguments.of(true, 1.0),
            Arguments.of(Missing.getInstance(), null),
            Arguments.of(null, null),
            Arguments.of(Instant.ofEpochMilli(1234567890L), 1234567890.0)
        );
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/172
    @ParameterizedTest
    @MethodSource("aggregateWithToDoubleArguments")
    void testAggregateWithToInt(Object given, Double expected) throws Exception {
        List<Document> pipeline = jsonList("$project: {value: {$toDouble: '$x'}}");

        Document document = json("_id: 1");
        if (!(given instanceof Missing)) {
            document.put("x", given);
        }
        collection.insertOne(document);

        assertThat(collection.aggregate(pipeline))
            .containsOnly(json("_id: 1").append("value", expected));
    }

    private static Stream<Arguments> aggregateWithToDoubleArguments_illegalValue() {
        return Stream.of(
            Arguments.of("abc", "'PlanExecutor error during aggregation :: caused by :: Failed to parse number 'abc' in $convert with no onError value"),
            Arguments.of(List.of(123), "'PlanExecutor error during aggregation :: caused by :: Unsupported conversion from array to double in $convert with no onError value'")
        );
    }

    @ParameterizedTest
    @MethodSource("aggregateWithToDoubleArguments_illegalValue")
    void testAggregateWithConvertToDouble_illegalValue(Object given, String expectedMessagePart) throws Exception {
        List<Document> pipeline = jsonList("$project: {value: {$toDouble: '$x'}}");

        collection.insertOne(json("_id: 1").append("x", given));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 241 (ConversionFailure): " + expectedMessagePart);
    }

    private static Stream<Arguments> aggregateWithToDateArguments() {
        return Stream.of(
            Arguments.of(Missing.getInstance(), null),
            Arguments.of(null, null),
            Arguments.of(1234567890L, Instant.ofEpochMilli(1234567890L)),
            Arguments.of(1234567890.0, Instant.ofEpochMilli(1234567890L)),
            Arguments.of("2020-07", Instant.parse("2020-07-01T00:00:00Z")),
            Arguments.of("2020-07-13", Instant.parse("2020-07-13T00:00:00Z")),
            Arguments.of("2020-07-13T14:30:57Z", Instant.parse("2020-07-13T14:30:57Z")),
            Arguments.of("2020-07-13T14:30:57.123Z", Instant.parse("2020-07-13T14:30:57.123Z")),
            Arguments.of("2020-07-13T14:30:57+0500", ZonedDateTime.parse("2020-07-13T14:30:57+05:00").toInstant()),
            Arguments.of("2020-07-13T14:30:57.123+0500", ZonedDateTime.parse("2020-07-13T14:30:57.123+05:00").toInstant()),
            Arguments.of("2020-07-13T14:30:57-0500", ZonedDateTime.parse("2020-07-13T14:30:57-05:00").toInstant()),
            Arguments.of(Instant.ofEpochMilli(1234567890L), Instant.ofEpochMilli(1234567890L))
        );
    }

    @ParameterizedTest
    @MethodSource("aggregateWithToDateArguments")
    void testAggregateWithToDate(Object given, Instant expected) throws Exception {
        List<Document> pipeline = jsonList("$project: {value: {$toDate: '$x'}}");

        Document document = json("_id: 1");
        if (!(given instanceof Missing)) {
            document.put("x", given);
        }
        collection.insertOne(document);

        assertThat(collection.aggregate(pipeline))
            .containsOnly(json("_id: 1").append("value", expected != null ? Date.from(expected) : null));
    }

    private static Stream<Arguments> aggregateWithToDateArguments_illegalValue() {
        return Stream.of(
            Arguments.of("abc", "'PlanExecutor error during aggregation :: caused by :: Error parsing date string 'abc';"),
            Arguments.of(123, "'PlanExecutor error during aggregation :: caused by :: Unsupported conversion from int to date in $convert with no onError value'"),
            Arguments.of("123456789", "'PlanExecutor error during aggregation :: caused by :: Error parsing date string '123456789';"),
            Arguments.of("2020-07-a", "'PlanExecutor error during aggregation :: caused by :: Error parsing date string '2020-07-a';"),
            Arguments.of(List.of(123), "'PlanExecutor error during aggregation :: caused by :: Unsupported conversion from array to date in $convert with no onError value'")
        );
    }

    @ParameterizedTest
    @MethodSource("aggregateWithToDateArguments_illegalValue")
    void testAggregateWithConvertToDate_illegalValue(Object given, String expectedMessagePart) throws Exception {
        List<Document> pipeline = jsonList("$project: {value: {$toDate: '$x'}}");

        collection.insertOne(json("_id: 1").append("x", given));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 241 (ConversionFailure): " + expectedMessagePart);
    }

    private static Stream<Arguments> aggregateWithToIntArguments() {
        return Stream.of(
            Arguments.of("12", 12),
            Arguments.of(9, 9),
            Arguments.of(false, 0),
            Arguments.of(true, 1),
            Arguments.of(Missing.getInstance(), null),
            Arguments.of(null, null)
        );
    }

    @ParameterizedTest
    @MethodSource("aggregateWithToIntArguments")
    void testAggregateWithToInt(Object given, Integer expected) throws Exception {
        List<Document> pipeline = jsonList("$project: {value: {$toInt: '$x'}}");

        Document document = json("_id: 1");
        if (!(given instanceof Missing)) {
            document.put("x", given);
        }
        collection.insertOne(document);

        assertThat(collection.aggregate(pipeline))
            .containsOnly(json("_id: 1").append("value", expected));
    }

    private static Stream<Arguments> aggregateWithToIntArguments_illegalValue() {
        return Stream.of(
            Arguments.of("abc", "'PlanExecutor error during aggregation :: caused by :: Failed to parse number 'abc' in $convert with no onError value"),
            Arguments.of(List.of(123), "'PlanExecutor error during aggregation :: caused by :: Unsupported conversion from array to int in $convert with no onError value'")
        );
    }

    @ParameterizedTest
    @MethodSource("aggregateWithToIntArguments_illegalValue")
    void testAggregateWithConvertToInt_illegalValue(Object given, String expectedMessagePart) throws Exception {
        List<Document> pipeline = jsonList("$project: {value: {$toInt: '$x'}}");

        collection.insertOne(json("_id: 1").append("x", given));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 241 (ConversionFailure): " + expectedMessagePart);
    }

    private static Stream<Arguments> aggregateWithToBoolArguments() {
        return Stream.of(
            Arguments.of("abc", true),
            Arguments.of(9, true),
            Arguments.of(0, false),
            Arguments.of(-1, true),
            Arguments.of(0L, false),
            Arguments.of(1L, true),
            Arguments.of(-1L, true),
            Arguments.of(-2L, true),
            Arguments.of(2L, true),
            Arguments.of(false, false),
            Arguments.of(true, true),
            Arguments.of(Missing.getInstance(), null),
            Arguments.of(null, null),
            Arguments.of(0.5, true),
            Arguments.of(0.1, true),
            Arguments.of(0.0, false),
            Arguments.of(-0.0, false),
            Arguments.of(-0.5, true),
            Arguments.of(5, true),
            Arguments.of(new ObjectId(), true),
            Arguments.of(Instant.ofEpochMilli(123456L), true),
            Arguments.of(List.of(false, true), true),
            Arguments.of(new ArrayList<>(), true)
        );
    }

    @ParameterizedTest
    @MethodSource("aggregateWithToBoolArguments")
    void testAggregateWithToBool(Object given, Boolean expected) throws Exception {
        List<Document> pipeline = jsonList("$project: {value: {$toBool: '$x'}}");

        Document document = json("_id: 1");
        if (!(given instanceof Missing)) {
            document.put("x", given);
        }
        collection.insertOne(document);

        assertThat(collection.aggregate(pipeline))
            .containsOnly(json("_id: 1").append("value", expected));
    }

    private static Stream<Arguments> aggregateWithToLongArguments() {
        return Stream.of(
            Arguments.of("12", 12L),
            Arguments.of(9, 9L),
            Arguments.of(9.5, 9L),
            Arguments.of(false, 0L),
            Arguments.of(true, 1L),
            Arguments.of(Missing.getInstance(), null),
            Arguments.of(null, null),
            Arguments.of(Instant.ofEpochMilli(123456789L), 123456789L)
        );
    }

    @ParameterizedTest
    @MethodSource("aggregateWithToLongArguments")
    void testAggregateWithToLong(Object given, Long expected) throws Exception {
        List<Document> pipeline = jsonList("$project: {value: {$toLong: '$x'}}");

        Document document = json("_id: 1");
        if (!(given instanceof Missing)) {
            document.put("x", given);
        }
        collection.insertOne(document);

        assertThat(collection.aggregate(pipeline))
            .containsOnly(json("_id: 1").append("value", expected));
    }

    private static Stream<Arguments> aggregateWithToLongArguments_illegalValue() {
        return Stream.of(
            Arguments.of("abc", "'PlanExecutor error during aggregation :: caused by :: Failed to parse number 'abc' in $convert with no onError value"),
            Arguments.of(List.of(123), "'PlanExecutor error during aggregation :: caused by :: Unsupported conversion from array to long in $convert with no onError value'")
        );
    }

    @ParameterizedTest
    @MethodSource("aggregateWithToLongArguments_illegalValue")
    void testAggregateWithConvertToLong_illegalValue(Object given, String expectedMessagePart) throws Exception {
        List<Document> pipeline = jsonList("$project: {value: {$toLong: '$x'}}");

        collection.insertOne(json("_id: 1").append("x", given));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 241 (ConversionFailure): " + expectedMessagePart);
    }

    private static Stream<Arguments> aggregateWithToObjectIdArguments() {
        return Stream.of(
            Arguments.of("5ab9cbfa31c2ab715d42129e", new ObjectId("5ab9cbfa31c2ab715d42129e")),
            Arguments.of(Missing.getInstance(), null),
            Arguments.of(null, null)
        );
    }

    @ParameterizedTest
    @MethodSource("aggregateWithToObjectIdArguments")
    void testAggregateWithToObjectId(Object given, ObjectId expected) throws Exception {
        List<Document> pipeline = jsonList("$project: {value: {$toObjectId: '$x'}}");

        Document document = json("_id: 1");
        if (!(given instanceof Missing)) {
            document.put("x", given);
        }
        collection.insertOne(document);

        assertThat(collection.aggregate(pipeline))
            .containsOnly(json("_id: 1").append("value", expected));
    }

    private static Stream<Arguments> aggregateWithToObjectIdArguments_illegalValue() {
        return Stream.of(
            Arguments.of("5ab9cbfa31c2ab715d42129", "'PlanExecutor error during aggregation :: caused by :: Failed to parse objectId '5ab9cbfa31c2ab715d42129' in $convert with no onError value"),
            Arguments.of("5ab9cbfa31c2ab715d42129z", "'PlanExecutor error during aggregation :: caused by :: Failed to parse objectId '5ab9cbfa31c2ab715d42129z' in $convert with no onError value"),
            Arguments.of(123, "'PlanExecutor error during aggregation :: caused by :: Unsupported conversion from int to objectId in $convert with no onError value'"),
            Arguments.of(List.of("5ab9cbfa31c2ab715d421290"), "'PlanExecutor error during aggregation :: caused by :: Unsupported conversion from array to objectId in $convert with no onError value'")
        );
    }

    @ParameterizedTest
    @MethodSource("aggregateWithToObjectIdArguments_illegalValue")
    void testAggregateWithConvertToObjectId_illegalValue(Object given, String expectedMessagePart) throws Exception {
        List<Document> pipeline = jsonList("$project: {value: {$toObjectId: '$x'}}");

        collection.insertOne(json("_id: 1").append("x", given));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageContaining("Command execution failed on MongoDB server with error 241 (ConversionFailure): " + expectedMessagePart);
    }

    private static Stream<Arguments> aggregateWithConvertArguments() {
        return Stream.of(
            Arguments.of("input: 1.5, to: null", null),

            Arguments.of("input: 1.5, to: 'double'", 1.5),
            Arguments.of("input: 1.5, to: 1", 1.5),

            Arguments.of("input: 25, to: 'string'", "25"),
            Arguments.of("input: 25, to: 2", "25"),

            Arguments.of("input: 'cafebabedeadbeefcafebabe', to: 'objectId'", new ObjectId("cafebabedeadbeefcafebabe")),
            Arguments.of("input: 'cafebabedeadbeefcafebabe', to: 7", new ObjectId("cafebabedeadbeefcafebabe")),

            Arguments.of("input: true, to: 'bool'", true),
            Arguments.of("input: true, to: 8", true),

            Arguments.of("input: '2020-10-07', to: 'date'", Date.from(Instant.parse("2020-10-07T00:00:00Z"))),
            Arguments.of("input: '2020-10-07', to: 9", Date.from(Instant.parse("2020-10-07T00:00:00Z"))),

            Arguments.of("input: '27', to: 'int'", 27),
            Arguments.of("input: '27', to: 16", 27),

            Arguments.of("input: '27', to: 'long'", 27L),
            Arguments.of("input: '27', to: 18", 27L),

            Arguments.of("input: '27.8', to: 'long', onError: 29", 29),
            Arguments.of("input: null, to: 'long', onNull: 29", 29)
        );
    }

    @ParameterizedTest
    @MethodSource("aggregateWithConvertArguments")
    void testAggregateWithConvert_literals(String given, Object expected) throws Exception {
        List<Document> pipeline = List.of(new Document("$project",
            new Document("value",
                new Document("$convert", json(given)))));

        Document document = json("_id: 1");
        collection.insertOne(document);

        assertThat(collection.aggregate(pipeline))
            .containsOnly(json("_id: 1").append("value", expected));
    }

    @ParameterizedTest
    @MethodSource("aggregateWithConvertArguments")
    void testAggregateWithConvert_indirect(String given, Object expected) throws Exception {
        Document convertDocument = json(given);
        Object inputValue = convertDocument.put("input", "$x");

        List<Document> pipeline = List.of(new Document("$project",
            new Document("value",
                new Document("$convert", convertDocument))));

        Document document = json("_id: 1").append("x", inputValue);
        collection.insertOne(document);

        assertThat(collection.aggregate(pipeline))
            .containsOnly(json("_id: 1").append("value", expected));
    }

    private static Stream<Arguments> aggregateWithConvertArguments_illegalValue() {
        return Stream.of(
            Arguments.of("input: 123, to: 'unknown'",
                "Command execution failed on MongoDB server with error 2 (BadValue): 'Failed to optimize pipeline :: caused by :: Unknown type name: unknown'"),

            Arguments.of("input: 123, to: 12.5",
                "Command execution failed on MongoDB server with error 9 (FailedToParse): 'Failed to optimize pipeline :: caused by :: In $convert, numeric 'to' argument is not an integer'"),

            Arguments.of("input: 123, to: [1, 2]",
                "Command execution failed on MongoDB server with error 9 (FailedToParse): 'Failed to optimize pipeline :: caused by :: $convert's 'to' argument must be a string or number, but is array'"),

            Arguments.of("x: 123",
                "Command execution failed on MongoDB server with error 9 (FailedToParse): 'Invalid $project :: caused by :: $convert found an unknown argument: x'"),

            Arguments.of("to: 'int'",
                "Command execution failed on MongoDB server with error 9 (FailedToParse): 'Invalid $project :: caused by :: Missing 'input' parameter to $convert'"),

            Arguments.of("input: 123, onError: 123",
                "Command execution failed on MongoDB server with error 9 (FailedToParse): 'Invalid $project :: caused by :: Missing 'to' parameter to $convert'"),

            Arguments.of("input: 123, to: 'int', onElse: 123",
                "Command execution failed on MongoDB server with error 9 (FailedToParse): 'Invalid $project :: caused by :: $convert found an unknown argument: onElse'")
        );
    }

    @ParameterizedTest
    @MethodSource("aggregateWithConvertArguments_illegalValue")
    void testAggregateWithConvert_illegalValue(String given, String expectedMessageStartingWith) throws Exception {
        List<Document> pipeline = List.of(new Document("$project",
            new Document("value", new Document("$convert", json(given)))));

        collection.insertOne(json("_id: 1"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageStartingWith(expectedMessageStartingWith);
    }

    @Test
    void testAggregateWithConvert_noDocument() throws Exception {
        List<Document> pipeline = List.of(new Document("$project",
            new Document("value",
                new Document("$convert", 123))));

        collection.insertOne(json("_id: 1"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(pipeline).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 9 (FailedToParse): 'Invalid $project :: caused by :: $convert expects an object of named arguments but found: int'");
    }

    @Test
    void testSampleAggregation() throws Exception {
        for (int i = 0; i < 20; i++) {
            collection.insertOne(json("flag: 1"));
        }

        assertThat(collection.aggregate(jsonList("$sample: { size: 3 }}", "$project: { _id: 0 }")))
            .containsExactly(
                json("flag: 1"),
                json("flag: 1"),
                json("flag: 1")
            );

        assertThat(collection.aggregate(jsonList("$sample: { size: 2.5 }}", "$project: { _id: 0 }")))
            .containsExactly(
                json("flag: 1"),
                json("flag: 1")
            );

        assertThat(collection.aggregate(jsonList("$sample: { size: 0 }}")))
            .isEmpty();

        assertThat(collection.aggregate(jsonList("$sample: { size: -0.3 }}")))
            .isEmpty();
    }

    @Test
    void testSampleAggregation_illegalParameters() throws Exception {
        collection.insertOne(json("_id: 1"));

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$sample: 3}")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 28745 (Location28745): 'the $sample stage specification must be an object'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$sample: [1, 2]}")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 28745 (Location28745): 'the $sample stage specification must be an object'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$sample: { size: 'a' }}")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 28746 (Location28746): 'size argument to $sample must be a number'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$sample: { size: null }}")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 28746 (Location28746): 'size argument to $sample must be a number'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$sample: { size: -1 }}")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 28747 (Location28747): 'size argument to $sample must not be negative'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$sample: { size: 1, bla: 2}}")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 28748 (Location28748): 'unrecognized option to $sample: bla'");

        assertThatExceptionOfType(MongoCommandException.class)
            .isThrownBy(() -> collection.aggregate(jsonList("$sample: {}}")).first())
            .withMessageStartingWith("Command execution failed on MongoDB server with error 28749 (Location28749): '$sample stage must specify a size'");
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
