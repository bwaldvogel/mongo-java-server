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
import static de.bwaldvogel.mongo.backend.DocumentBuilder.notIn;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.or;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.regex;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.size;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.junit.Test;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class DefaultQueryMatcherTest {

    private QueryMatcher matcher = new DefaultQueryMatcher();

    @Test
    public void testMatchesSimple() throws Exception {
        assertThat(matcher.matches(json(""), json(""))).isTrue();
        assertThat(matcher.matches(json(""), json("foo: 'bar'"))).isFalse();
        assertThat(matcher.matches(json("foo: 'bar'"), json("foo: 'bar'"))).isTrue();
    }

    @Test
    public void testMatchesNullOrMissing() throws Exception {
        assertThat(matcher.matches(json("x: null"), json("x: null"))).isTrue();
        assertThat(matcher.matches(json(""), json("x: null"))).isTrue();
    }

    @Test
    public void testMatchesPattern() throws Exception {
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
    public void testMatchesInQuery() throws Exception {
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
    public void testMatchesGreaterThanQuery() throws Exception {
        assertThat(matcher.matches(json(""), map("a", gt(-1)))).isFalse();
        assertThat(matcher.matches(json("a: 1"), map("a", gt(0.9)))).isTrue();
        assertThat(matcher.matches(json("a: 1"), map("a", gt(0)))).isTrue();
        assertThat(matcher.matches(json("a: 1"), map("a", gt(1)))).isFalse();
        assertThat(matcher.matches(json("a: 1"), map("a", gte(1)))).isTrue();
        assertThat(matcher.matches(json("a: 'x'"), map("a", gt(1)))).isFalse();
        assertThat(matcher.matches(json("a: 'x'"), map("a", gte(1)))).isFalse();
    }

    @Test
    public void testMatchesLessThanQuery() throws Exception {
        assertThat(matcher.matches(json(""), map("a", lt(-1)))).isFalse();
        assertThat(matcher.matches(json("a: 1"), map("a", lt(1.001)))).isTrue();
        assertThat(matcher.matches(json("a: 1"), map("a", lt(2)))).isTrue();
        assertThat(matcher.matches(json("a: 1"), map("a", lt(1)))).isFalse();
        assertThat(matcher.matches(json("a: 1"), map("a", lte(1)))).isTrue();
        assertThat(matcher.matches(json("a: 'x'"), map("a", lt(1)))).isFalse();
        assertThat(matcher.matches(json("a: 'x'"), map("a", lte(1)))).isFalse();
    }

    @Test
    public void testMatchesValueList() throws Exception {
        Document document = json("a: [1, 2, 3]");
        assertThat(matcher.matches(document, json(""))).isTrue();
        assertThat(matcher.matches(document, json("a: 1"))).isTrue();
        assertThat(matcher.matches(document, json("a: 2"))).isTrue();
        assertThat(matcher.matches(document, json("a: 3"))).isTrue();
        assertThat(matcher.matches(document, json("a: 4"))).isFalse();
    }

    @Test
    public void testMatchesDocumentList() throws Exception {
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
    public void testMatchesSubquery() throws Exception {
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
    public void testMatchesMissingEmbeddedDocument() throws Exception {
        assertThat(matcher.matches(json("b: null"), json("'b.c': null"))).isTrue();
        assertThat(matcher.matches(json("b: {c: null}"), json("'b.c': null"))).isTrue();
        assertThat(matcher.matches(json("b: {c: 123}"), json("'b.c': null"))).isFalse();
        assertThat(matcher.matches(json("b: {c: []}"), json("'b.c': null"))).isFalse();
        assertThat(matcher.matches(json("b: {c: [1, 2, 3]}"), json("'b.c': null"))).isFalse();
        assertThat(matcher.matches(json("b: {c: [1, null, 3]}"), json("'b.c': null"))).isTrue();
        assertThat(matcher.matches(json("b: {x: 'foo'}"), json("'b.c': null"))).isTrue();
        assertThat(matcher.matches(json("b: {x: 'foo', c: null}"), json("'b.c': null"))).isTrue();
        assertThat(matcher.matches(json(""), json("'b.c': null"))).isTrue();
    }

    @Test
    public void testMatchesSubqueryList() throws Exception {
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
    public void testMatchesSubqueryListPosition() throws Exception {
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
    public void testInvalidOperator() throws Exception {
        Document document = json("");
        Document query = json("field: {$someInvalidOperator: 123}");

        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> matcher.matches(document, query))
            .withMessage("[Error 2] unknown operator: $someInvalidOperator");
    }

    @Test
    public void testMatchesExists() throws Exception {
        Document document = json("");
        Document query = map("qty", exists().appendAll(notIn(5, 15)));

        assertThat(matcher.matches(document, query)).isFalse();

        document.put("qty", 17);
        assertThat(matcher.matches(document, query)).isTrue();

        document.put("qty", 15);
        assertThat(matcher.matches(document, query)).isFalse();
    }

    @Test
    public void testMatchesNotEqual() throws Exception {
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
    public void testMatchesEqual() throws Exception {
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
    public void testMatchesEqualEmbeddedDocument() throws Exception {
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
    public void testMatchesEqualOneArrayValue() throws Exception {
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
    public void testMatchesEqualTwoArrayValues() throws Exception {
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

    @Test
    public void testMatchesNot() throws Exception {

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

    /**
     * Test case for https://github.com/bwaldvogel/mongo-java-server/issues/7
     */
    @Test
    public void testMatchesNotIn() throws Exception {
        Document query1 = map("map.key2", notIn("value 2.2"));
        Document query2 = map("map.key2", not(in("value 2.2")));
        Document query3 = map("map.key2", not(notIn("value 2.2")));
        Document query4 = map("map.key2", not(not(in("value 2.2"))));

        Document document1 = json("code: 'c1', map: {key1: 'value 1.1', key2: ['value 2.1']}");
        Document document2 = json("code: 'c1', map: {key1: 'value 1.2', key2: ['value 2.2']}");
        Document document3 = json("code: 'c1', map: {key1: 'value 2.1', key2: ['value 2.1']}");

        assertThat(matcher.matches(document1, query1)).isTrue();
        assertThat(matcher.matches(document2, query1)).isFalse();
        assertThat(matcher.matches(document3, query1)).isTrue();

        assertThat(matcher.matches(document1, query2)).isTrue();
        assertThat(matcher.matches(document2, query2)).isFalse();
        assertThat(matcher.matches(document3, query2)).isTrue();

        assertThat(matcher.matches(document1, query3)).isFalse();
        assertThat(matcher.matches(document2, query3)).isTrue();
        assertThat(matcher.matches(document3, query3)).isFalse();

        assertThat(matcher.matches(document1, query4)).isFalse();
        assertThat(matcher.matches(document2, query4)).isTrue();
        assertThat(matcher.matches(document3, query4)).isFalse();

        assertThat(matcher.matches(json("values: [1, 2, 3]"), json("values: {$nin: []}"))).isTrue();
        assertThat(matcher.matches(json("values: null"), json("values: {$nin: []}"))).isTrue();
        assertThat(matcher.matches(json(""), json("values: {$nin: []}"))).isTrue();
        assertThat(matcher.matches(json(""), json("values: {$nin: [1]}"))).isTrue();
        assertThat(matcher.matches(json(""), json("values: {$nin: [1, 2]}"))).isTrue();
        assertThat(matcher.matches(json("values: null"), json("values: {$nin: [null]}"))).isFalse();
        assertThat(matcher.matches(json(""), json("values: {$nin: [null]}"))).isFalse();
    }

    @Test
    public void testMatchesNotSize() throws Exception {
        assertThat(matcher.matches(json("a: [1, 2, 3]"), json("a: {$not: {$size: 3}}"))).isFalse();
        assertThat(matcher.matches(json("a: [1, 2, 3]"), json("a: {$not: {$size: 4}}"))).isTrue();
    }

    @Test
    public void testMatchesNotPattern() throws Exception {

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
    public void testMatchesAnd() throws Exception {

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
    public void testMatchesOr() throws Exception {

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
    public void testMatchesNor() throws Exception {

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
    public void testMatchesIllegalQueryAndOrNor() throws Exception {

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
    public void testMatchesMod() throws Exception {
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
    public void testMatchesSize() throws Exception {

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
    public void testMatchesAll() throws Exception {

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
    public void testMatchesAllSubdocument() throws Exception {
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
    public void testMatchesAllWithEmptyCollection() throws Exception {
        Document query = json("$and: [{'text': 'TextA'}, {'tags': {$all: []}}]");

        assertThat(matcher.matches(json("text: 'TextA', tags: []"), query)).isFalse();
        assertThat(matcher.matches(json("text: 'TextB', tags: []"), query)).isFalse();
        assertThat(matcher.matches(json("text: 'TextA', tags: ['A']"), query)).isFalse();
    }

    @Test
    public void testMatchesAllAndIn() throws Exception {
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
    public void testMatchesAllAndNin() throws Exception {
        Document query = json("$and: [{'tags': {$all: ['A']}}, {'tags': {$nin: ['B', 'C']}}]");

        assertThat(matcher.matches(json("tags: ['A', 'D']"), query)).isTrue();
        assertThat(matcher.matches(json("tags: ['A', 'B']"), query)).isFalse();
        assertThat(matcher.matches(json("tags: ['A', 'C']"), query)).isFalse();
        assertThat(matcher.matches(json("tags: ['C', 'D']"), query)).isFalse();
    }

    @Test
    public void testMatchesAllAndNotIn() throws Exception {
        Document query = json("$and: [{'tags': {$all: ['A']}}, {'tags': {$not: {$in: ['B', 'C']}}}]");

        assertThat(matcher.matches(json("tags: ['A', 'D']"), query)).isTrue();
        assertThat(matcher.matches(json("tags: ['A', 'B']"), query)).isFalse();
        assertThat(matcher.matches(json("tags: ['A', 'C']"), query)).isFalse();
        assertThat(matcher.matches(json("tags: ['C', 'D']"), query)).isFalse();
    }

    @Test
    public void testMatchesElement() throws Exception {
        Document document1 = json("results: [82, 85, 88]");
        assertThat(matcher.matches(document1, map("results", elemMatch(gte(80))))).isTrue();
        assertThat(matcher.matches(document1, map("results", elemMatch(gte(80).appendAll(lt(85)))))).isTrue();
        assertThat(matcher.matches(document1, map("results", elemMatch(gte(90).appendAll(lt(85)))))).isFalse();
        assertThat(matcher.matches(document1, map("results", elemMatch(gte(70).appendAll(lt(80)))))).isFalse();

        Document document2 = json("_id: 2, results: [75, 88, 89]");
        assertThat(matcher.matches(document2, map("results", elemMatch(gte(80).appendAll(lt(85)))))).isFalse();
    }

    @Test
    public void testMatchesElementInEmbeddedDocuments() throws Exception {
        Document document1 = json("_id: 1, results: [{product: 'abc', score: 10}, {product: 'xyz', score: 5}]");
        Document document2 = json("_id: 2, results: [{product: 'abc', score:  9}, {product: 'xyz', score: 7}]");
        Document document3 = json("_id: 3, results: [{product: 'abc', score:  7}, {product: 'xyz', score: 8}]");

        Document query = map("results", elemMatch(json("product: 'xyz'").append("score", gte(8))));
        assertThat(matcher.matches(document1, query)).isFalse();
        assertThat(matcher.matches(document2, query)).isFalse();
        assertThat(matcher.matches(document3, query)).isTrue();

        Document hasProductXyzElement = map("results", elemMatch(json("product: 'xyz'")));
        assertThat(matcher.matches(document1, hasProductXyzElement)).isTrue();
        assertThat(matcher.matches(document2, hasProductXyzElement)).isTrue();
        assertThat(matcher.matches(document3, hasProductXyzElement)).isTrue();
    }

    @Test
    public void testEmptyMatchesElementQuery() throws Exception {
        Document document = json("_id: 1, results: [{product: 'xyz', score: 5}]");
        assertThat(matcher.matches(document, map("results", elemMatch(json(""))))).isTrue();
    }

    @Test
    public void testMatchesExpr() throws Exception {
        assertThat(matcher.matches(json(""), json("$expr: true"))).isTrue();
        assertThat(matcher.matches(json(""), json("$expr: false"))).isFalse();
        assertThat(matcher.matches(json("value: 1"), json("$expr: '$value'"))).isTrue();
        assertThat(matcher.matches(json("value: 1"), json("$expr: {$not: '$value'}"))).isFalse();
        assertThat(matcher.matches(json("value: 0"), json("$expr: '$value'"))).isFalse();
    }

}
