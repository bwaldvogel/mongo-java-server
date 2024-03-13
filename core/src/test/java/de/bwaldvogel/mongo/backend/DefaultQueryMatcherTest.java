package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.TestUtils.json;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.allOf;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.and;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.elemMatch;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.exists;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.gt;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.gte;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.in;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.list;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.lt;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.lte;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.map;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.mod;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.nor;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.not;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.or;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.regex;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.size;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

import de.bwaldvogel.mongo.bson.BinData;
import de.bwaldvogel.mongo.bson.BsonJavaScript;
import de.bwaldvogel.mongo.bson.BsonTimestamp;
import de.bwaldvogel.mongo.bson.Decimal128;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.MaxKey;
import de.bwaldvogel.mongo.bson.MinKey;
import de.bwaldvogel.mongo.bson.ObjectId;
import de.bwaldvogel.mongo.exception.BadValueException;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerNotYetImplementedException;

class DefaultQueryMatcherTest {

    private final QueryMatcher matcher = new DefaultQueryMatcher();

    @Test
    void testMatchesSimple() throws Exception {
        assertThat(matcher.matches(json(""), json(""))).isTrue();
        assertThat(matcher.matches(json(""), json("foo: 'bar'"))).isFalse();
        assertThat(matcher.matches(json("foo: 'bar'"), json("foo: 'bar'"))).isTrue();
    }

    @Test
    void testIllegalQuery() throws Exception {
        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> matcher.matches(json(""), json("x: {$lt: 10, y: 23}")))
            .withMessage("[Error 2] unknown operator: y");
    }

    @Test
    void testLegalQueryWithOperatorAndWithoutOperator() throws Exception {
        List<Document> documents = List.of(
            json(""),
            json("x: 23"),
            json("x: {y: 23}"),
            json("x: {y: {z: 23}}"),
            json("a: 123, x: {y: {z: 23}}")
        );
        for (Document document : documents) {
            assertThat(matcher.matches(document, json("x: {y: 23, $lt: 10}"))).isFalse();
            assertThat(matcher.matches(document, json("x: {y: {$lt: 100, z: 23}}"))).isFalse();
            assertThat(matcher.matches(document, json("a: 123, x: {y: {$lt: 100, z: 23}}"))).isFalse();
        }
    }

    @Test
    void testMatchesNullOrMissing() throws Exception {
        assertThat(matcher.matches(json("x: null"), json("x: null"))).isTrue();
        assertThat(matcher.matches(json(""), json("x: null"))).isTrue();
    }

    @Test
    void testMatchesPattern() throws Exception {
        Document document = json("name: 'john'");
        assertThat(matcher.matches(document, map("name", regex("jo.*")))).isTrue();
        assertThat(matcher.matches(document, map("name", regex("Jo.*", "i")))).isTrue();
        assertThat(matcher.matches(document, map("name", regex("marta")))).isFalse();
        assertThat(matcher.matches(document, map("name", regex("John")))).isFalse();

        String name = "\u0442\u0435\u0441\u0442";
        assertThat(name).hasSize(4);
        document.put("name", name);
        assertThat(matcher.matches(document, map("name", regex(name)))).isTrue();

        assertThat(matcher.matches(document, map("name", regex(name)))).isTrue();

        document.put("name", name.toLowerCase());
        assertThat(name.toLowerCase()).isNotEqualTo(name.toUpperCase());

        assertThat(matcher.matches(document,
            map("name", regex(name.toUpperCase())))).isFalse();

        assertThat(matcher.matches(document,
            map("name", regex(name.toUpperCase(), "i")))).isTrue();
    }

    @Test
    void testMatchesInQuery() throws Exception {
        Document query = map("a", in(3, 2, 1));
        assertThat(matcher.matches(json(""), query)).isFalse();
        assertThat(matcher.matches(json("a: 'x'"), query)).isFalse();
        assertThat(matcher.matches(json("a: 1"), query)).isTrue();
        assertThat(matcher.matches(json("a: 2"), query)).isTrue();
        assertThat(matcher.matches(json("a: 4"), query)).isFalse();
        assertThat(matcher.matches(json("a: 1.0"), query)).isTrue();
        Document otherQuery = map("a", in(3.0, 2.0, 1.00001));
        assertThat(matcher.matches(json("a: 1"), otherQuery)).isFalse();
    }

    @Test
    void testMatchesGreaterThanQuery() throws Exception {
        assertThat(matcher.matches(json(""), map("a", gt(-1)))).isFalse();
        assertThat(matcher.matches(json("a: 1"), map("a", gt(0.9)))).isTrue();
        assertThat(matcher.matches(json("a: 1"), map("a", gt(0)))).isTrue();
        assertThat(matcher.matches(json("a: 1"), map("a", gt(1)))).isFalse();
        assertThat(matcher.matches(json("a: 1"), map("a", gte(1)))).isTrue();
        assertThat(matcher.matches(json("a: 'x'"), map("a", gt(1)))).isFalse();
        assertThat(matcher.matches(json("a: 'x'"), map("a", gte(1)))).isFalse();
    }

    @Test
    void testMatchesLessThanQuery() throws Exception {
        assertThat(matcher.matches(json(""), map("a", lt(-1)))).isFalse();
        assertThat(matcher.matches(json("a: 1"), map("a", lt(1.001)))).isTrue();
        assertThat(matcher.matches(json("a: 1"), map("a", lt(2)))).isTrue();
        assertThat(matcher.matches(json("a: 1"), map("a", lt(1)))).isFalse();
        assertThat(matcher.matches(json("a: 1"), map("a", lte(1)))).isTrue();
        assertThat(matcher.matches(json("a: 'x'"), map("a", lt(1)))).isFalse();
        assertThat(matcher.matches(json("a: 'x'"), map("a", lte(1)))).isFalse();
    }

    @Test
    void testMatchesLessThanAndGreaterThanQuery() throws Exception {
        Document query = json("x: { $lt : { n: 'a' , t: 10}, $gt: { n: 'a', t: 1}}}");

        assertThat(matcher.matches(json("x: {n: 'a', t: 1}"), query)).isFalse();
        assertThat(matcher.matches(json("x: {n: 'a', t: 2}"), query)).isTrue();
        assertThat(matcher.matches(json("x: {n: 'a', t: 11}"), query)).isFalse();
    }

    @Test
    void testMatchesValueList() throws Exception {
        Document document = json("a: [1, 2, 3]");
        assertThat(matcher.matches(document, json(""))).isTrue();
        assertThat(matcher.matches(document, json("a: 1"))).isTrue();
        assertThat(matcher.matches(document, json("a: 2"))).isTrue();
        assertThat(matcher.matches(document, json("a: 3"))).isTrue();
        assertThat(matcher.matches(document, json("a: 4"))).isFalse();
    }

    @Test
    void testMatchesLists() throws Exception {
        assertThat(matcher.matches(json("a: [2, 1]"), json("a: [2, 1]"))).isTrue();
        assertThat(matcher.matches(json("a: [2, 1]"), json("a: [2, 1.0]"))).isTrue();
        assertThat(matcher.matches(json("a: [2.0, 1]"), json("a: [2, 1.0]"))).isTrue();
        assertThat(matcher.matches(json("a: [1, 2]"), json("a: [2, 1]"))).isFalse();
    }

    @Test
    void testMatchesDocumentList() throws Exception {
        Document document = json("_id: 1, c: [{a: 1, b: 2}, {a: 3, b: 4}]");

        assertThat(matcher.matches(document, json(""))).isTrue();
        assertThat(matcher.matches(document, json("c: 1"))).isFalse();
        assertThat(matcher.matches(document, json("'c.a': 1"))).isTrue();
        assertThat(matcher.matches(document, json("'c.a': 3"))).isTrue();

        assertThat(matcher.matches(document, json("'c.a': 1").append("c.b", 4))).isTrue();
        assertThat(matcher.matches(document, json("'c.a': 1").append("c.b", 5))).isFalse();
        assertThat(matcher.matches(document, json("'c.a': 2").append("c.b", 4))).isFalse();
    }

    @Test
    void testMatchesSubquery() throws Exception {
        Document document = json("c: {a: 1}");
        assertThat(matcher.matches(document, json(""))).isTrue();
        assertThat(matcher.matches(document, json("c: 1"))).isFalse();
        assertThat(matcher.matches(document, json("'c.a': 1"))).isTrue();
        assertThat(matcher.matches(document, json("'c.a': 2"))).isFalse();
        assertThat(matcher.matches(document, json("'c.a.x': 2"))).isFalse();

        assertThat(matcher.matches(json("c: 5"), json("'c.a.x': 2"))).isFalse();
        assertThat(matcher.matches(json(""), json("'c.a.x': 2"))).isFalse();

        document.putAll(json("a: {b: {c: {d: 1}}}"));
        assertThat(matcher.matches(document, json("'a.b.c.d': 1"))).isTrue();
        assertThat(matcher.matches(document, json("'a.b': 1"))).isFalse();
        assertThat(matcher.matches(document, json("'a.b.c': {d: 1}"))).isTrue();
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/35
    @Test
    void testMatchesMissingEmbeddedDocument() throws Exception {
        assertThat(matcher.matches(json(""), json("b: {c: null}"))).isFalse();
        assertThat(matcher.matches(json("b: null"), json("b: {c: null}"))).isFalse();
        assertThat(matcher.matches(json("b: null"), json("'b.c': null"))).isTrue();
        assertThat(matcher.matches(json("b: {c: null}"), json("'b.c': null"))).isTrue();
        assertThat(matcher.matches(json("b: {c: null}"), json("b: {c: null}"))).isTrue();
        assertThat(matcher.matches(json("b: {c: null}"), json("b: {c: null, d: null}"))).isFalse();
        assertThat(matcher.matches(json("b: {c: 123}"), json("'b.c': null"))).isFalse();
        assertThat(matcher.matches(json("b: {c: []}"), json("'b.c': null"))).isFalse();
        assertThat(matcher.matches(json("b: {c: [1, 2, 3]}"), json("'b.c': null"))).isFalse();
        assertThat(matcher.matches(json("b: {c: [1, null, 3]}"), json("b: {c: null}"))).isFalse();
        assertThat(matcher.matches(json("b: {c: [1, null, 3]}"), json("'b.c': null"))).isTrue();
        assertThat(matcher.matches(json("b: {x: 'foo'}"), json("'b.c': null"))).isTrue();
        assertThat(matcher.matches(json("b: {x: 'foo', c: null}"), json("'b.c': null"))).isTrue();
        assertThat(matcher.matches(json(""), json("'b.c': null"))).isTrue();
        assertThat(matcher.matches(json("b: {c: ['a', null, 'b']}"), json("'b.c': null"))).isTrue();
        assertThat(matcher.matches(json("b: {c: ['a', null, 'b']}"), json("b: {c: null}"))).isFalse();
    }

    @Test
    void testMatchesEmbeddedDocument() throws Exception {
        assertThat(matcher.matches(json("b: {c: 1, d: 2}"), json("b: {c: 1}"))).isFalse();
        assertThat(matcher.matches(json("b: {c: 1}"), json("b: {c: 1, d: 1}"))).isFalse();
        assertThat(matcher.matches(json("b: {c: 0}"), json("b: {c: 0}"))).isTrue();
        assertThat(matcher.matches(json("b: {c: 0}"), json("b: {c: 0.0}"))).isTrue();
        assertThat(matcher.matches(json("b: {c: 0}"), json("b: {c: -0.0}"))).isTrue();
        assertThat(matcher.matches(json("b: {c: 1, d: 2}"), json("'b.c': 1"))).isTrue();
        assertThat(matcher.matches(json("b: {c: 1, d: 2}"), json("'b.c': 1.0"))).isTrue();

        assertThat(matcher.matches(json("b: {c: [1, 2, 3]}"), json("'b.c': 1"))).isTrue();
        assertThat(matcher.matches(json("b: {c: [1, 2, 3]}"), json("'b.c': 1.0"))).isTrue();
        assertThat(matcher.matches(json("b: {c: [1, 2, 3]}"), json("b: {c: 1}"))).isFalse();
    }

    @Test
    void testMatchesSubqueryList() throws Exception {
        Document document = json("c: {a: [1, 2, 3]}");
        assertThat(matcher.matches(document, json(""))).isTrue();

        assertThat(matcher.matches(document, json("c: 1"))).isFalse();
        assertThat(matcher.matches(document, json("'c.a': 1"))).isTrue();
        assertThat(matcher.matches(document, json("'c.a': 2"))).isTrue();
        assertThat(matcher.matches(document, json("'c.a': 3"))).isTrue();
        assertThat(matcher.matches(document, json("'c.a': 4"))).isFalse();
        assertThat(matcher.matches(document, json("'c.a.x': 1"))).isFalse();

        document = json("array: [{'123a': {name: 'old'}}]");
        Document query = map("array.123a.name", "old");
        assertThat(matcher.matches(document, query)).isTrue();
    }

    @Test
    void testMatchesSubqueryListPosition() throws Exception {
        Document document = json("c: {a: [1, 2, 3]}");
        assertThat(matcher.matches(document, json(""))).isTrue();
        assertThat(matcher.matches(document, json("c: 1"))).isFalse();
        assertThat(matcher.matches(document, json("'c.a.0': 1"))).isTrue();
        assertThat(matcher.matches(document, json("'c.a.0': 2"))).isFalse();

        document = json("c: [{a: 12}, {a: 13}]");
        assertThat(matcher.matches(document, json("'c.a': 12"))).isTrue();
        assertThat(matcher.matches(document, json("'c.a': 13"))).isTrue();
        assertThat(matcher.matches(document, json("'c.a': 14"))).isFalse();
        assertThat(matcher.matches(document, json("'c.0.a': 12"))).isTrue();
        assertThat(matcher.matches(document, json("'c.0.a': 13"))).isFalse();
        assertThat(matcher.matches(document, json("'c.1.a': 12"))).isFalse();
        assertThat(matcher.matches(document, json("'c.1.a': 13"))).isTrue();
        assertThat(matcher.matches(document, json("'c.2.a': 13"))).isFalse();
    }

    @Test
    void testInvalidOperator() throws Exception {
        Document document = json("");
        Document query = json("field: {$someInvalidOperator: 123}");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> matcher.matches(document, query))
            .withMessage("[Error 2] unknown operator: $someInvalidOperator");
    }

    @Test
    void testMatchesExists() throws Exception {
        Document query = json("qty: {$exists: true, $nin: [5, 15]}");
        assertThat(matcher.matches(json(""), query)).isFalse();
        assertThat(matcher.matches(json("qty: 17"), query)).isTrue();
        assertThat(matcher.matches(json("qty: 15"), query)).isFalse();

        assertThat(matcher.matches(json("a: {b: 1}"), json("'a.b': {$exists: true}"))).isTrue();
        assertThat(matcher.matches(json("a: 1"), json("'a.b': {$exists: true}"))).isFalse();
        assertThat(matcher.matches(json("a: 1"), json("b: {$exists: true}"))).isFalse();
        assertThat(matcher.matches(json("a: 1"), json("'a.': {$exists: true}"))).isFalse();
        assertThat(matcher.matches(json("a: 1"), json("'.a': {$exists: true}"))).isFalse();
        assertThat(matcher.matches(json("a: {b: 1}"), json("b: {$exists: true}"))).isFalse();
        assertThat(matcher.matches(json("a: null"), json("'a.b': {$exists: true}"))).isFalse();
        assertThat(matcher.matches(json("a: null"), json("'a.b': {$exists: false}"))).isTrue();
    }

    @Test
    void testMatchesExistsArray() throws Exception {
        assertThat(matcher.matches(json("a: ['X', 'Y', 'Z']"), json("'a.1': {$exists: true}"))).isTrue();
        assertThat(matcher.matches(json("a: ['X', 'Y', 'Z']"), json("'a.5': {$exists: true}"))).isFalse();
        assertThat(matcher.matches(json("a: ['X', 'Y', 'Z']"), json("'a.5': {$exists: false}"))).isTrue();
        assertThat(matcher.matches(json("a: ['X', 'Y', 'Z']"), json("'a.0.1': {$exists: true}"))).isFalse();
        assertThat(matcher.matches(json("a: ['X', 'Y', 'Z']"), json("'a.0.1': {$exists: false}"))).isTrue();
        assertThat(matcher.matches(json("a: ['X', 'Y', 'Z']"), json("'a.0.b': {$exists: true}"))).isFalse();
        assertThat(matcher.matches(json("a: ['X', 'Y', 'Z']"), json("'a.0.b': {$exists: false}"))).isTrue();
        assertThat(matcher.matches(json("a: [[1, 2], [3, 4]]"), json("'a.0.1': {$exists: true}"))).isTrue();
        assertThat(matcher.matches(json("a: [[1, 2], [3, 4]]"), json("'a.0.1': {$exists: false}"))).isFalse();
        assertThat(matcher.matches(json("a: [{b: 'c'}]"), json("'a.0.b': {$exists: true}"))).isTrue();
        assertThat(matcher.matches(json("a: [{b: 'c'}]"), json("'a.0.c': {$exists: true}"))).isFalse();
        assertThat(matcher.matches(json("a: [{c: 'd'}]"), json("'a.0.b': {$exists: false}"))).isTrue();
        assertThat(matcher.matches(json("a: [{c: 'd'}]"), json("'a.0.c': {$exists: false}"))).isFalse();
        assertThat(matcher.matches(json("a: null"), json("'a.1': {$exists: false}"))).isTrue();
        assertThat(matcher.matches(json("a: null"), json("'a.1': {$exists: true}"))).isFalse();
        assertThat(matcher.matches(json(""), json("'a.1': {$exists: false}"))).isTrue();
        assertThat(matcher.matches(json(""), json("'a.1': {$exists: true}"))).isFalse();
        assertThat(matcher.matches(json("a: [1, 2, 3]"), json("'a.b': {$exists: false}"))).isTrue();
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/53
    @Test
    void testMatchesExistsTrailingDot() throws Exception {
        assertThat(matcher.matches(json("a: ['X', 'Y', 'Z']"), json("'a.': {$exists: true}"))).isTrue();
        assertThat(matcher.matches(json("a: ['X', 'Y', 'Z']"), json("'a..': {$exists: true}"))).isFalse();
        assertThat(matcher.matches(json("a: 123"), json("'a.': {$exists: true}"))).isFalse();
        assertThat(matcher.matches(json("a: []"), json("'a.': {$exists: true}"))).isTrue();
        assertThat(matcher.matches(json("a: [[1, 2, 3]]"), json("'a.0.': {$exists: true}"))).isTrue();
        assertThat(matcher.matches(json("a: [[1, 2, 3]]"), json("'a.1.': {$exists: true}"))).isFalse();
        assertThat(matcher.matches(json("a: ['X', 'Y', 'Z']"), json("'a..': {$exists: true}"))).isFalse();
    }

    @Test
    void testMatchesNotEqual() throws Exception {
        Document document = json("");

        Document query = json("qty: {$ne: 17}");

        assertThat(matcher.matches(document, query)).isTrue();

        document.put("qty", 17);
        assertThat(matcher.matches(document, query)).isFalse();

        document.put("qty", 15);
        assertThat(matcher.matches(document, query)).isTrue();
    }

    // http://docs.mongodb.org/v3.0/reference/operator/query/eq/#op._S_eq
    @Test
    void testMatchesEqual() throws Exception {
        Document document1 = json("_id: 1, item: {name: 'ab', code: '123'}, qty: 15, tags: ['A', 'B', 'C']");
        Document document2 = json("_id: 2, item: {name: 'cd', code: '123'}, qty: 20, tags: ['B']");
        Document document3 = json("_id: 3, item: {name: 'ij', code: '456'}, qty: 25, tags: ['A', 'B']");
        Document document4 = json("_id: 4, item: {name: 'xy', code: '456'}, qty: 30, tags: ['B', 'A']");
        Document document5 = json("_id: 5, item: {name: 'mn', code: '000'}, qty: 20, tags: [['A', 'B'], 'C']");

        Document query = json("qty: {$eq: 20}");

        assertThat(matcher.matches(document1, query)).isFalse();
        assertThat(matcher.matches(document2, query)).isTrue();
        assertThat(matcher.matches(document3, query)).isFalse();
        assertThat(matcher.matches(document4, query)).isFalse();
        assertThat(matcher.matches(document5, query)).isTrue();
    }

    // http://docs.mongodb.org/v3.0/reference/operator/query/eq/#op._S_eq
    @Test
    void testMatchesEqualEmbeddedDocument() throws Exception {
        Document document1 = json("_id: 1, item: {name: 'ab', code: '123'}, qty: 15, tags: ['A', 'B', 'C']");
        Document document2 = json("_id: 2, item: {name: 'cd', code: '123'}, qty: 20, tags: ['B']");
        Document document3 = json("_id: 3, item: {name: 'ij', code: '456'}, qty: 25, tags: ['A', 'B']");
        Document document4 = json("_id: 4, item: {name: 'xy', code: '456'}, qty: 30, tags: ['B', 'A']");
        Document document5 = json("_id: 5, item: {name: 'mn', code: '000'}, qty: 20, tags: [['A', 'B'], 'C']");

        Document query = json("'item.name': {$eq: 'ab'}");

        assertThat(matcher.matches(document1, query)).isTrue();
        assertThat(matcher.matches(document2, query)).isFalse();
        assertThat(matcher.matches(document3, query)).isFalse();
        assertThat(matcher.matches(document4, query)).isFalse();
        assertThat(matcher.matches(document5, query)).isFalse();
    }

    // http://docs.mongodb.org/v3.0/reference/operator/query/eq/#op._S_eq
    @Test
    void testMatchesEqualOneArrayValue() throws Exception {
        Document document1 = json("_id: 1, item: {name: 'ab', code: '123'}, qty: 15, tags: ['A', 'B', 'C']");
        Document document2 = json("_id: 2, item: {name: 'cd', code: '123'}, qty: 20, tags: ['B']");
        Document document3 = json("_id: 3, item: {name: 'ij', code: '456'}, qty: 25, tags: ['A', 'B']");
        Document document4 = json("_id: 4, item: {name: 'xy', code: '456'}, qty: 30, tags: ['B', 'A']");
        Document document5 = json("_id: 5, item: {name: 'mn', code: '000'}, qty: 20, tags: [['A', 'B'], 'C']");

        Document query = json("tags: {$eq: 'B'}");

        assertThat(matcher.matches(document1, query)).isTrue();
        assertThat(matcher.matches(document2, query)).isTrue();
        assertThat(matcher.matches(document3, query)).isTrue();
        assertThat(matcher.matches(document4, query)).isTrue();
        assertThat(matcher.matches(document5, query)).isFalse();
    }

    // http://docs.mongodb.org/v3.0/reference/operator/query/eq/#op._S_eq
    @Test
    void testMatchesEqualTwoArrayValues() throws Exception {
        Document document1 = json("_id: 1, item: {name: 'ab', code: '123'}, qty: 15, tags: ['A', 'B', 'C']");
        Document document2 = json("_id: 2, item: {name: 'cd', code: '123'}, qty: 20, tags: ['B']");
        Document document3 = json("_id: 3, item: {name: 'ij', code: '456'}, qty: 25, tags: ['A', 'B']");
        Document document4 = json("_id: 4, item: {name: 'xy', code: '456'}, qty: 30, tags: ['B', 'A']");
        Document document5 = json("_id: 5, item: {name: 'mn', code: '000'}, qty: 20, tags: [['A', 'B'], 'C']");

        Document query = json("tags: {$eq: ['A', 'B']}");

        assertThat(matcher.matches(document1, query)).isFalse();
        assertThat(matcher.matches(document2, query)).isFalse();
        assertThat(matcher.matches(document3, query)).isTrue();
        assertThat(matcher.matches(document4, query)).isFalse();
        assertThat(matcher.matches(document5, query)).isTrue();
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/45
    @Test
    void testMatchesNotEqualArrayValue() throws Exception {
        assertThat(matcher.matches(json("a: [-1]"), json("a: {$ne: 0}"))).isTrue();
        assertThat(matcher.matches(json("a: -1"), json("a: {$ne: 0}"))).isTrue();

        assertThat(matcher.matches(json("a: [0]"), json("a: {$ne: 0}"))).isFalse();
        assertThat(matcher.matches(json("a: [0.0]"), json("a: {$ne: 0}"))).isFalse();
        assertThat(matcher.matches(json("a: [0, 1]"), json("a: {$ne: 0}"))).isFalse();
        assertThat(matcher.matches(json("a: [-0.0]"), json("a: {$ne: 0}"))).isFalse();
        assertThat(matcher.matches(json("a: 0"), json("a: {$ne: 0}"))).isFalse();
    }

    @Test
    void testMatchNotEqualArray() throws Exception {
        assertThat(matcher.matches(json("a: []"), json("a: {$ne: []}"))).isFalse();
        assertThat(matcher.matches(json("a: null"), json("a: {$ne: []}"))).isTrue();
        assertThat(matcher.matches(json(""), json("a: {$ne: []}"))).isTrue();
        assertThat(matcher.matches(json("a: 1"), json("a: {$ne: []}"))).isTrue();
        assertThat(matcher.matches(json("a: [1, 2, 3]"), json("a: {$ne: []}"))).isTrue();
        assertThat(matcher.matches(json("a: [1, 2, 3]"), json("a: {$ne: [1, 2, 3]}"))).isFalse();
        assertThat(matcher.matches(json("a: [1, 2]"), json("a: {$ne: [1, 2, 3]}"))).isTrue();
    }

    @Test
    void testMatchesNotEqualAndSize() throws Exception {
        assertThat(matcher.matches(json("a: [1, 2]"), json("a: {$ne: [1, 3], $size: 2}"))).isTrue();
        assertThat(matcher.matches(json("a: [1, 2]"), json("a: {$ne: [1, 2], $size: 2}"))).isFalse();
        assertThat(matcher.matches(json("a: [1, 2]"), json("a: {$size: 2, $ne: [1, 2]}"))).isFalse();
        assertThat(matcher.matches(json("a: [1, 2]"), json("a: {$ne: [2, 3], $size: 3}"))).isFalse();
    }

    @Test
    void testMatchEqualArray() throws Exception {
        assertThat(matcher.matches(json("a: []"), json("a: {$eq: []}"))).isTrue();
        assertThat(matcher.matches(json("a: null"), json("a: {$eq: []}"))).isFalse();
        assertThat(matcher.matches(json(""), json("a: {$eq: []}"))).isFalse();
        assertThat(matcher.matches(json("a: 1"), json("a: {$eq: []}"))).isFalse();
        assertThat(matcher.matches(json("a: [1, 2, 3]"), json("a: {$eq: []}"))).isFalse();
        assertThat(matcher.matches(json("a: [1, 2, 3]"), json("a: {$eq: [1, 2, 3]}"))).isTrue();
        assertThat(matcher.matches(json("a: [1, 2]"), json("a: {$eq: [1, 2, 3]}"))).isFalse();
    }

    @Test
    void testMatchesNot() throws Exception {

        Document query = map("price", not(gt(1.99)));

        /*
         * This query will select all documents in the inventory collection
         * where:
         *
         * the price field value is less than or equal to 1.99 or the price
         * field does not exist
         *
         * { $not: { $gt: 1.99 } } is different from the $lte operator. { $lte:
         * 1.99 } returns only the documents where price field exists and its
         * value is less than or equal to 1.99.
         */

        Document document = json("");
        assertThat(matcher.matches(document, query)).isTrue();

        document.put("price", 1.99);
        assertThat(matcher.matches(document, query)).isTrue();

        document.put("price", 1.990001);
        assertThat(matcher.matches(document, query)).isFalse();

        // !(x >= 5 && x <= 7)
        query = map("price", not(gte(5).appendAll(lte(7))));
        assertThat(matcher.matches(document, query)).isTrue();
        document.put("price", 5);
        assertThat(matcher.matches(document, query)).isFalse();
        document.put("price", 7);
        assertThat(matcher.matches(document, query)).isFalse();
        document.put("price", null);
        assertThat(matcher.matches(document, query)).isTrue();

        query = map("price", not(exists()));
        assertThat(matcher.matches(document, query)).isFalse();
        document.remove("price");
        assertThat(matcher.matches(document, query)).isTrue();
    }

    @Test
    void testMatchesNotSize() throws Exception {
        assertThat(matcher.matches(json("a: [1, 2, 3]"), json("a: {$not: {$size: 3}}"))).isFalse();
        assertThat(matcher.matches(json("a: [1, 2, 3]"), json("a: {$not: {$size: 4}}"))).isTrue();
    }

    @Test
    void testMatchesNotPattern() throws Exception {

        // { item: { $not: /^p.*/ } }
        Document query = map("item", not(regex("^p.*")));

        Document document = json("");
        assertThat(matcher.matches(document, query)).isTrue();
        document.put("item", "p");
        assertThat(matcher.matches(document, query)).isFalse();
        document.put("item", "pattern");
        assertThat(matcher.matches(document, query)).isFalse();
        document.put("item", "Pattern");
        assertThat(matcher.matches(document, query)).isTrue();
    }

    @Test
    void testMatchesAnd() throws Exception {

        Document query = and(json("price: 1.99"), map("qty", lt(20)).append("sale", true));

        /*
         * This query will select all documents in the inventory collection
         * where:
         *
         * price field value equals 1.99 and qty field value is less than 20 and
         * sale field value is equal to true.
         */

        Document document = json("");
        assertThat(matcher.matches(document, query)).isFalse();

        document.put("price", 1.99);
        document.put("qty", 20);
        document.put("sale", false);
        assertThat(matcher.matches(document, query)).isFalse();

        document.put("qty", 19);
        assertThat(matcher.matches(document, query)).isFalse();
        document.put("sale", true);
        assertThat(matcher.matches(document, query)).isTrue();
    }

    @Test
    void testMatchesOr() throws Exception {

        Document query = or(json("price: 1.99"), map("qty", lt(20)));

        Document document = json("");
        assertThat(matcher.matches(document, query)).isFalse();

        document.put("price", 1.99);
        assertThat(matcher.matches(document, query)).isTrue();

        document.put("price", 2.00);
        assertThat(matcher.matches(document, query)).isFalse();

        document.put("qty", 19);
        assertThat(matcher.matches(document, query)).isTrue();
    }

    @Test
    void testMatchesNor() throws Exception {

        Document query = nor(json("price: 1.99"), map("qty", lt(20)));

        Document document = json("");
        assertThat(matcher.matches(document, query)).isTrue();

        document.put("price", 1.99);
        assertThat(matcher.matches(document, query)).isFalse();
        document.put("price", 2.00);
        assertThat(matcher.matches(document, query)).isTrue();

        document.put("qty", 19);
        assertThat(matcher.matches(document, query)).isFalse();
        document.put("qty", 20);
        assertThat(matcher.matches(document, query)).isTrue();
    }

    @Test
    void testMatchesIllegalQueryAndOrNor() throws Exception {

        for (QueryFilter op : new QueryFilter[] { QueryFilter.AND, QueryFilter.OR, QueryFilter.NOR }) {
            assertNonEmptyArrayException(map(op.getValue(), null));
            assertNonEmptyArrayException(map(op.getValue(), 2));
            assertNonEmptyArrayException(map(op.getValue(), 2));

            assertThatExceptionOfType(MongoServerError.class)
                .isThrownBy(() -> matcher.matches(json(""), map(op, "a")))
                .withMessage("[Error 14817] " + op + " elements must be objects");
        }
    }

    private void assertNonEmptyArrayException(Document query) throws Exception {
        Document emptyDocument = json("");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> matcher.matches(emptyDocument, query))
            .withMessage("[Error 2] $and/$or/$nor must be a nonempty array");
    }

    @Test
    void testMatchesMod() throws Exception {
        Document document = json("");

        Document query = map("x", mod(4, 0));
        assertThat(matcher.matches(document, query)).isFalse();

        for (int m = 0; m < 4; m++) {
            query = map("x", mod(4, m));
            for (int i = 0; i < 20; i++) {
                document.put("x", i);
                assertThat(matcher.matches(document, query)).isEqualTo((i % 4) == m);
            }
        }
    }

    @Test
    void testMatchesSize() throws Exception {

        Document query = map("a", size(1));

        assertThat(matcher.matches(json(""), query)).isFalse();
        assertThat(matcher.matches(json("a: 'x'"), query)).isFalse();
        assertThat(matcher.matches(map("a", list()), query)).isFalse();
        assertThat(matcher.matches(map("a", list(1)), query)).isTrue();
        assertThat(matcher.matches(map("a", list(1, 2)), query)).isFalse();

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> matcher.matches(json(""), json("a: {$size: {$gt: 0}}")))
            .withMessage("[Error 2] $size needs a number");
    }

    @Test
    void testMatchesAll() throws Exception {

        Document document = map("a", map("x", list()));

        assertThat(matcher.matches(document, map("x", allOf(1)))).isFalse();

        assertThat(matcher.matches(document, map("a.x", allOf()))).isFalse();
        assertThat(matcher.matches(document, map("a.x", allOf(1)))).isFalse();

        document = map("a", map("x", list(1, 2, 3)));
        assertThat(matcher.matches(document, map("a", allOf(1)))).isFalse();
        assertThat(matcher.matches(document, map("a.x", allOf()))).isFalse();
        assertThat(matcher.matches(document, map("a.x", allOf(1)))).isTrue();
        assertThat(matcher.matches(document, map("a.y", allOf(1)))).isFalse();
        assertThat(matcher.matches(document, map("a.x", allOf(2)))).isTrue();
        assertThat(matcher.matches(document, map("a.x", allOf(3)))).isTrue();
        assertThat(matcher.matches(document, map("a.x", allOf(1, 2, 3)))).isTrue();
        assertThat(matcher.matches(document, map("a.x", allOf(2, 3)))).isTrue();
        assertThat(matcher.matches(document, map("a.x", allOf(1, 3)))).isTrue();
        assertThat(matcher.matches(document, map("a.x", allOf(1, 4)))).isFalse();
        assertThat(matcher.matches(document, map("a.x", allOf(4)))).isFalse();

        // with regular expresssion
        document = map("a", map("x", list("john", "jo", "maria")));
        assertThat(matcher.matches(document, map("a.x", allOf()))).isFalse();
        assertThat(matcher.matches(document, map("a.x", allOf(regex("^jo.*"))))).isTrue();
        assertThat(matcher.matches(document, map("a.x", allOf(regex("^foo"))))).isFalse();
        assertThat(matcher.matches(document, map("a.x", allOf("maria", regex("^jo.*"))))).isTrue();
    }

    @Test
    void testMatchesAllSubdocument() throws Exception {
        // with subdocuments
        Document document = map("a", list(json("x: 1"), json("x: 2")));
        assertThat(matcher.matches(document, map("a.x", allOf()))).isTrue();
        assertThat(matcher.matches(document, map("a.x", allOf(1)))).isTrue();
        assertThat(matcher.matches(document, map("a.x", allOf(2)))).isTrue();
        assertThat(matcher.matches(document, map("a.x", allOf(1, 2)))).isTrue();
        assertThat(matcher.matches(document, map("a.x", allOf(2, 3)))).isFalse();
        assertThat(matcher.matches(document, map("a.x", allOf(3)))).isFalse();
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/36
    @Test
    void testMatchesAllWithEmptyCollection() throws Exception {
        Document query = json("$and: [{'text': 'TextA'}, {'tags': {$all: []}}]");

        assertThat(matcher.matches(json("text: 'TextA', tags: []"), query)).isFalse();
        assertThat(matcher.matches(json("text: 'TextB', tags: []"), query)).isFalse();
        assertThat(matcher.matches(json("text: 'TextA', tags: ['A']"), query)).isFalse();
    }

    @Test
    void testMatchesAllAndIn() throws Exception {
        Document document = map("a", map("x", list(1, 3)));
        assertThat(matcher.matches(document, map("a.x", allOf(1, 3).appendAll(in(2))))).isFalse();
        assertThat(matcher.matches(document, map("a.x", allOf(1, 3).appendAll(in(3))))).isTrue();

        document = map("a", map("x", list(1, 2, 3)));
        assertThat(matcher.matches(document, map("a.x", allOf(1, 3).appendAll(in(2))))).isTrue();

        document = map("a", list(json("x: 1"), json("x: 2"), json("x: 3")));
        assertThat(matcher.matches(document, map("a.x", allOf(1, 3).appendAll(in(2))))).isTrue();
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/36
    @Test
    void testMatchesAllAndNin() throws Exception {
        Document query = json("$and: [{'tags': {$all: ['A']}}, {'tags': {$nin: ['B', 'C']}}]");

        assertThat(matcher.matches(json("tags: ['A', 'D']"), query)).isTrue();
        assertThat(matcher.matches(json("tags: ['A', 'B']"), query)).isFalse();
        assertThat(matcher.matches(json("tags: ['A', 'C']"), query)).isFalse();
        assertThat(matcher.matches(json("tags: ['C', 'D']"), query)).isFalse();
    }

    @Test
    void testMatchesAllAndNotIn() throws Exception {
        Document query = json("$and: [{'tags': {$all: ['A']}}, {'tags': {$not: {$in: ['B', 'C']}}}]");

        assertThat(matcher.matches(json("tags: ['A', 'D']"), query)).isTrue();
        assertThat(matcher.matches(json("tags: ['A', 'B']"), query)).isFalse();
        assertThat(matcher.matches(json("tags: ['A', 'C']"), query)).isFalse();
        assertThat(matcher.matches(json("tags: ['C', 'D']"), query)).isFalse();
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/96
    @Test
    void testMatchesAllAndSize() throws Exception {
        Document query = json("list: {$all: ['A', 'B'], $size: 2}");

        assertThat(matcher.matches(json("list: ['A', 'B']"), query)).isTrue();
        assertThat(matcher.matches(json("list: ['A', 'D']"), query)).isFalse();
        assertThat(matcher.matches(json("list: ['A', 'B', 'C']"), query)).isFalse();
        assertThat(matcher.matches(json("list: 123"), query)).isFalse();
        assertThat(matcher.matches(json(""), query)).isFalse();
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/96
    @Test
    void testMatchesSizeAndAll() throws Exception {
        Document query = json("list: {$size: 2, $all: ['A', 'B']}");

        assertThat(matcher.matches(json("list: ['A', 'D']"), query)).isFalse();
        assertThat(matcher.matches(json("list: ['A', 'B']"), query)).isTrue();
        assertThat(matcher.matches(json("list: ['A', 'B', 'C']"), query)).isFalse();
        assertThat(matcher.matches(json("list: 123"), query)).isFalse();
        assertThat(matcher.matches(json(""), query)).isFalse();
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/97
    @Test
    void testMatchesAllElements() throws Exception {
        Document document = json("list: [{a: 'b'}, {c: 'd'}]");

        assertThat(matcher.matches(document, json("list: {$all: [{$elemMatch: {a: 'b'}}]}"))).isTrue();
        assertThat(matcher.matches(document, json("list: {$all: [{$elemMatch: {a: 'b'}}, {$elemMatch: {c: 'd'}}]}"))).isTrue();
        assertThat(matcher.matches(document, json("list: {$all: [{$elemMatch: {a: 'b'}}, {$elemMatch: {e: 'f'}}]}"))).isFalse();
        assertThat(matcher.matches(document, json("list: {$all: [{$elemMatch: {a: 'b'}}, {$elemMatch: {c: 'd'}}, {$elemMatch: {e: 'f'}}]}"))).isFalse();
    }

    @Test
    void testMatchesElement() throws Exception {
        Document document1 = json("results: [82, 85, 88]");
        assertThat(matcher.matches(document1, map("results", elemMatch(gte(80))))).isTrue();
        assertThat(matcher.matches(document1, map("results", elemMatch(gte(80).appendAll(lt(85)))))).isTrue();
        assertThat(matcher.matches(document1, map("results", elemMatch(gte(90).appendAll(lt(85)))))).isFalse();
        assertThat(matcher.matches(document1, map("results", elemMatch(gte(70).appendAll(lt(80)))))).isFalse();

        Document document2 = json("_id: 2, results: [75, 88, 89]");
        assertThat(matcher.matches(document2, map("results", elemMatch(gte(80).appendAll(lt(85)))))).isFalse();

        assertThat(matcher.matches(json("a: [{b: 'c'}]"), json("a: {$elemMatch: {b: 'c'}}"))).isTrue();
        assertThat(matcher.matches(json("a: [{b: 'c'}, {c: 'd'}]"), json("a: {$elemMatch: {b: 'c'}}"))).isTrue();
        assertThat(matcher.matches(json("a: [{c: 'd'}, {e: 'f'}]"), json("a: {$elemMatch: {b: 'c'}}"))).isFalse();
        assertThat(matcher.matches(json("a: {b: 'c'}"), json("a: {$elemMatch: {b: 'c'}}"))).isFalse();
        assertThat(matcher.matches(json("a: null"), json("a: {$elemMatch: {b: 'c'}}"))).isFalse();
        assertThat(matcher.matches(json(""), json("a: {$elemMatch: {b: 'c'}}"))).isFalse();
        assertThat(matcher.matches(json("a: [{b: 'c'}]"), json("a: {$elemMatch: {b: 'd'}}"))).isFalse();
        assertThat(matcher.matches(json("a: [{b: {c: 'd'}}]"), json("a: {$elemMatch: {b: 'd'}}"))).isFalse();
    }

    @Test
    void testMatchesElementInEmbeddedDocuments() throws Exception {
        Document document1 = json("_id: 1, results: [{product: 'abc', score: 10}, {product: 'xyz', score: 5}]");
        Document document2 = json("_id: 2, results: [{product: 'abc', score:  9}, {product: 'xyz', score: 7}]");
        Document document3 = json("_id: 3, results: [{product: 'abc', score:  7}, {product: 'xyz', score: 8}]");

        Document query = json("results: {$elemMatch: {product: 'xyz', score: {$gte: 8}}}");
        assertThat(matcher.matches(document1, query)).isFalse();
        assertThat(matcher.matches(document2, query)).isFalse();
        assertThat(matcher.matches(document3, query)).isTrue();

        Document hasProductXyzElement = json("results: {$elemMatch: {product: 'xyz'}}");
        assertThat(matcher.matches(document1, hasProductXyzElement)).isTrue();
        assertThat(matcher.matches(document2, hasProductXyzElement)).isTrue();
        assertThat(matcher.matches(document3, hasProductXyzElement)).isTrue();
    }

    @Test
    void testMatchesElementWithQueryFilter() throws Exception {
        Document document1 = json("a: [{v: 'X'}, {v: 'Y'}]");
        Document document2 = json("a: [{v: 'Z'}]");

        assertThat(matcher.matches(document1, json("a: {$elemMatch: {$or: [{v: 'X'}, {v: 'Y'}]}}"))).isTrue();
        assertThat(matcher.matches(document2, json("a: {$elemMatch: {$or: [{v: 'X'}, {v: 'Y'}]}}"))).isFalse();

        assertThat(matcher.matches(document1, json("a: {$elemMatch: {$and: [{v: 'Y'}, {v: {$ne: 'Z'}}]}}"))).isTrue();
        assertThat(matcher.matches(document2, json("a: {$elemMatch: {$and: [{v: 'Y'}, {v: {$ne: 'Z'}}]}}"))).isFalse();

        assertThat(matcher.matches(document1, json("a: {$elemMatch: {$nor: [{v: 'X'}, {v: 'Y'}]}}"))).isFalse();
        assertThat(matcher.matches(document2, json("a: {$elemMatch: {$nor: [{v: 'X'}, {v: 'Y'}]}}"))).isTrue();
    }

    @Test
    void testEmptyMatchesElementQuery() throws Exception {
        Document document = json("_id: 1, results: [{product: 'xyz', score: 5}]");
        assertThat(matcher.matches(document, map("results", elemMatch(json(""))))).isTrue();
    }

    @Test
    void testMatchesExpr() throws Exception {
        assertThat(matcher.matches(json(""), json("$expr: true"))).isTrue();
        assertThat(matcher.matches(json(""), json("$expr: false"))).isFalse();
        assertThat(matcher.matches(json("value: 1"), json("$expr: '$value'"))).isTrue();
        assertThat(matcher.matches(json("value: 1"), json("$expr: {$not: '$value'}"))).isFalse();
        assertThat(matcher.matches(json("value: 0"), json("$expr: '$value'"))).isFalse();
    }

    @Test
    void testMatchesZeroValues() throws Exception {
        assertThat(matcher.matches(json("v: 0"), json("v: 0.0"))).isTrue();
        assertThat(matcher.matches(json("v: 0.0"), json("v: 0.0"))).isTrue();
        assertThat(matcher.matches(json("v: 0.0"), json("v: 0"))).isTrue();
        assertThat(matcher.matches(json("v: -0.0"), json("v: 0"))).isTrue();
        assertThat(matcher.matches(json("v: -0.0"), json("v: -0.0"))).isTrue();
        assertThat(matcher.matches(json("v: 0.0"), json("v: -0.0"))).isTrue();
    }

    @Test
    void testMatchesType() throws Exception {
        assertThat(matcher.matches(json("v: 0"), json("v: {$type: 16}"))).isTrue();
        assertThat(matcher.matches(json("v: 'abc'"), json("v: {$type: 16}"))).isFalse();
        assertThat(matcher.matches(json("v: 0"), json("v: {$type: 16.0}"))).isTrue();

        assertThat(matcher.matches(json("v: 0"), json("v: {$type: [2, 16]}"))).isTrue();
        assertThat(matcher.matches(json("v: 'abc'"), json("v: {$type: ['int', 'long']}"))).isFalse();

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> matcher.matches(json("v: 0"), json("v: {$type: []}")))
            .withMessage("[Error 9] v must match at least one type");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> matcher.matches(json("a: 0"), json("'a.b.c': {$type: []}")))
            .withMessage("[Error 9] a.b.c must match at least one type");

        assertThat(matcher.matches(json("v: 0"), json("v: {$type: 'number'}"))).isTrue();
        assertThat(matcher.matches(json("v: 0.0"), json("v: {$type: 'number'}"))).isTrue();
        assertThat(matcher.matches(new Document("v", Decimal128.ONE), json("v: {$type: 'number'}"))).isTrue();
        assertThat(matcher.matches(json("v: 0"), json("v: {$type: 'decimal'}"))).isFalse();
        assertThat(matcher.matches(new Document("v", Decimal128.ONE), json("v: {$type: 'decimal'}"))).isTrue();

        assertThatExceptionOfType(BadValueException.class)
            .isThrownBy(() -> DefaultQueryMatcher.matchTypes("abc", "abc"))
            .withMessage("[Error 2] Unknown type name alias: abc");

        assertThatExceptionOfType(BadValueException.class)
            .isThrownBy(() -> DefaultQueryMatcher.matchTypes("abc", 16.3))
            .withMessage("[Error 2] Invalid numerical type code: 16.3");

        assertThat(DefaultQueryMatcher.matchTypes(1.0, 1)).isTrue();
        assertThat(DefaultQueryMatcher.matchTypes(1, 1)).isFalse();
        assertThat(DefaultQueryMatcher.matchTypes("abc", 2)).isTrue();
        assertThat(DefaultQueryMatcher.matchTypes(new Document(), 3)).isTrue();
        assertThat(DefaultQueryMatcher.matchTypes(List.of(1, 2, 3), 4)).isTrue();
        assertThat(DefaultQueryMatcher.matchTypes(UUID.randomUUID(), 5)).isTrue();
        assertThat(DefaultQueryMatcher.matchTypes(new BinData( new byte[] { 1, 2, 3 }), 5)).isTrue();
        assertThat(DefaultQueryMatcher.matchTypes(new ObjectId(), 7)).isTrue();
        assertThat(DefaultQueryMatcher.matchTypes(true, 8)).isTrue();
        assertThat(DefaultQueryMatcher.matchTypes(Boolean.FALSE, 8)).isTrue();
        assertThat(DefaultQueryMatcher.matchTypes(Instant.now(), 9)).isTrue();
        assertThat(DefaultQueryMatcher.matchTypes(null, 10)).isTrue();
        assertThat(DefaultQueryMatcher.matchTypes(Pattern.compile(".*"), 11)).isTrue();
        assertThat(DefaultQueryMatcher.matchTypes(123, 16)).isTrue();
        assertThat(DefaultQueryMatcher.matchTypes(new BsonTimestamp(123), 17)).isTrue();
        assertThat(DefaultQueryMatcher.matchTypes(123L, 16)).isFalse();
        assertThat(DefaultQueryMatcher.matchTypes(123, 18)).isFalse();
        assertThat(DefaultQueryMatcher.matchTypes(123L, 18)).isTrue();
        assertThat(DefaultQueryMatcher.matchTypes(MinKey.getInstance(), -1)).isTrue();
        assertThat(DefaultQueryMatcher.matchTypes(MaxKey.getInstance(), 127)).isTrue();
    }

    @Test
    void testMatchesJavaScript() throws Exception {
        Document document = new Document("data", new BsonJavaScript("code 1"));
        assertThat(matcher.matches(document, new Document("data", new BsonJavaScript("code 1")))).isTrue();
        assertThat(matcher.matches(document, new Document("data", new BsonJavaScript("code 2")))).isFalse();
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/132
    @Test
    void testNearSphere() throws Exception {
        assertThatExceptionOfType(MongoServerNotYetImplementedException.class)
            .isThrownBy(() -> matcher.matches(json("location: [0, 0]"), json("location: " +
                "{" +
                "    $nearSphere: {" +
                "        $geometry: {" +
                "            type : 'Point'," +
                "            coordinates : [ -73.9667, 40.78 ]" +
                "        },\n" +
                "        $minDistance: 1000,\n" +
                "        $maxDistance: 5000\n" +
                "    }" +
                "}")))
            .withMessage("$nearSphere is not yet implemented. See https://github.com/bwaldvogel/mongo-java-server/issues/132");
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/132
    @Test
    void testGeoWithin() throws Exception {
        assertThatExceptionOfType(MongoServerNotYetImplementedException.class)
            .isThrownBy(() -> matcher.matches(json("location: [0, 0]"), json("location: {" +
                "    $geoWithin: {" +
                "        $geometry: {" +
                "            type: 'Polygon'," +
                "            coordinates: [[[0, 0], [3, 6], [6, 1], [0, 0]]]," +
                "            crs: {" +
                "                type: 'name'," +
                "                properties: { name: 'urn:x-mongodb:crs:strictwinding:EPSG:4326' }" +
                "            }" +
                "        }" +
                "    }" +
                "}")))
            .withMessage("$geoWithin is not yet implemented. See https://github.com/bwaldvogel/mongo-java-server/issues/132");
    }

}
