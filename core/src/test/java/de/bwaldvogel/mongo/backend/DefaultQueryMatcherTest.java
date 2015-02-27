package de.bwaldvogel.mongo.backend;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

import de.bwaldvogel.mongo.exception.MongoServerError;

public class DefaultQueryMatcherTest {

    private QueryMatcher matcher;

    private BasicDBObject json(String string) {
        string = string.trim();
        if (!string.startsWith("{")) {
            string = "{" + string + "}";
        }
        return (BasicDBObject) JSON.parse(string);
    }

    @Before
    public void setUp() {
        matcher = new DefaultQueryMatcher();
    }

    @Test
    public void testMatchesSimple() throws Exception {
        BSONObject document = json("{}");
        assertThat(matcher.matches(document, json("{}"))).isTrue();
        assertThat(matcher.matches(document, json("foo: 'bar'"))).isFalse();
        document.put("foo", "bar");
        assertThat(matcher.matches(document, json("foo: 'bar'"))).isTrue();
    }

    @Test
    public void testMatchesPattern() throws Exception {
        BSONObject document = new BasicDBObject("name", "john");
        assertThat(matcher.matches(document, new BasicBSONObject("name", Pattern.compile("jo.*")))).isTrue();
        assertThat(
                matcher.matches(document,
                        new BasicBSONObject("name", Pattern.compile("Jo.*", Pattern.CASE_INSENSITIVE)))).isTrue();
        assertThat(matcher.matches(document, new BasicBSONObject("name", Pattern.compile("marta")))).isFalse();
        assertThat(matcher.matches(document, new BasicBSONObject("name", Pattern.compile("John")))).isFalse();

        String name = "\u0442\u0435\u0441\u0442";
        assertThat(name).hasSize(4);
        document.put("name", name);
        assertThat(matcher.matches(document, new BasicBSONObject("name", Pattern.compile(name)))).isTrue();

        assertThat(matcher.matches(document, new BasicBSONObject("name", new BasicBSONObject("$regex", name))))
                .isTrue();

        document.put("name", name.toLowerCase());
        assertThat(name.toLowerCase()).isNotEqualTo(name.toUpperCase());
        assertThat(
                matcher.matches(document,
                        new BasicBSONObject("name", new BasicBSONObject("$regex", name.toUpperCase())))).isFalse();
        assertThat(
                matcher.matches(
                        document,
                        new BasicBSONObject("name", new BasicBSONObject("$regex", name.toUpperCase()).append(
                                "$options", "i")))).isTrue();
    }

    @Test
    public void testMatchesInQuery() throws Exception {
        assertThat(matcher.matches(json("{}"), json("a: {$in: [3,2,1]}"))).isFalse();
        assertThat(matcher.matches(json("a: 'x'"), json("a: {$in: [3,2,1]}"))).isFalse();
        assertThat(matcher.matches(json("a: 1"), json("a: {$in: [3,2,1]}"))).isTrue();
        assertThat(matcher.matches(json("a: 2"), json("a: {$in: [3,2,1]}"))).isTrue();
        assertThat(matcher.matches(json("a: 4"), json("a: {$in: [3,2,1]}"))).isFalse();
        assertThat(matcher.matches(json("a: 1.0"), json("a: {$in: [3,2,1]}"))).isTrue();
        assertThat(matcher.matches(json("a: 1"), json("a: {$in: [3.0, 2.0, 1.00001]}"))).isFalse();
    }

    @Test
    public void testMatchesGreaterThanQuery() throws Exception {
        assertThat(matcher.matches(json("{}"), json("a: {$gt: -1}"))).isFalse();
        assertThat(matcher.matches(json("a: 1"), json("a: {$gt: 0.9}"))).isTrue();
        assertThat(matcher.matches(json("a: 1"), json("a: {$gt: 0}"))).isTrue();
        assertThat(matcher.matches(json("a: 1"), json("a: {$gt: 1}"))).isFalse();
        assertThat(matcher.matches(json("a: 1"), json("a: {$gte: 1}"))).isTrue();
        assertThat(matcher.matches(new BasicDBObject("a", "x"), json("a: {$gt: 1}"))).isFalse();
        assertThat(matcher.matches(new BasicDBObject("a", "x"), json("a: {$gte: 1}"))).isFalse();
    }

    @Test
    public void testMatchesLessThanQuery() throws Exception {
        assertThat(matcher.matches(json("{}"), json("a: {$lt: -1}"))).isFalse();
        assertThat(matcher.matches(json("a: 1"), json("a: {$lt: 1.001}"))).isTrue();
        assertThat(matcher.matches(json("a: 1"), json("a: {$lt: 2}"))).isTrue();
        assertThat(matcher.matches(json("a: 1"), json("a: {$lt: 1}"))).isFalse();
        assertThat(matcher.matches(json("a: 1"), json("a: {$lte: 1}"))).isTrue();
        assertThat(matcher.matches(new BasicDBObject("a", "x"), json("a: {$lt: 1}"))).isFalse();
        assertThat(matcher.matches(new BasicDBObject("a", "x"), json("a: {$lte: 1}"))).isFalse();
    }

    @Test
    public void testMatchesValueList() throws Exception {
        BSONObject document = json("a: [1,2,3]");
        assertThat(matcher.matches(document, json("{}"))).isTrue();
        assertThat(matcher.matches(document, json("a: 1"))).isTrue();
        assertThat(matcher.matches(document, json("a: 2"))).isTrue();
        assertThat(matcher.matches(document, json("a: 3"))).isTrue();
        assertThat(matcher.matches(document, json("a: 4"))).isFalse();
    }

    @Test
    public void testMatchesDocumentList() throws Exception {
        BSONObject document = json("_id:1, c: [{a:1, b:2}, {a:3, b:4}]");

        assertThat(matcher.matches(document, json("{}"))).isTrue();
        assertThat(matcher.matches(document, json("c: 1"))).isFalse();
        assertThat(matcher.matches(document, json("'c.a': 1"))).isTrue();
        assertThat(matcher.matches(document, json("'c.a': 3"))).isTrue();

        assertThat(matcher.matches(document, json("'c.a': 1, 'c.b': 4"))).isTrue();
        assertThat(matcher.matches(document, json("'c.a': 1, 'c.b': 5"))).isFalse();
        assertThat(matcher.matches(document, json("'c.a': 2, 'c.b': 4"))).isFalse();
    }

    @Test
    public void testMatchesSubquery() throws Exception {
        BSONObject document = json("c: {a:1}");
        assertThat(matcher.matches(document, json("{}"))).isTrue();
        assertThat(matcher.matches(document, json("c: 1"))).isFalse();
        assertThat(matcher.matches(document, json("'c.a': 1"))).isTrue();
        assertThat(matcher.matches(document, json("'c.a': 2"))).isFalse();
        assertThat(matcher.matches(document, json("'c.a.x': 2"))).isFalse();

        document.put("a", json("b: {c: {d:1}}"));
        assertThat(matcher.matches(document, json("'a.b.c.d': 1"))).isTrue();
        assertThat(matcher.matches(document, json("'a.b': 1"))).isFalse();
        assertThat(matcher.matches(document, json("'a.b.c': {d:1}"))).isTrue();
    }

    @Test
    public void testMatchesSubqueryList() throws Exception {
        BSONObject document = json("c: {a: [1,2,3] }");
        assertThat(matcher.matches(document, json("{}"))).isTrue();

        assertThat(matcher.matches(document, json("c: 1"))).isFalse();
        assertThat(matcher.matches(document, json("'c.a': 1"))).isTrue();
        assertThat(matcher.matches(document, json("'c.a': 2"))).isTrue();
        assertThat(matcher.matches(document, json("'c.a': 3"))).isTrue();
        assertThat(matcher.matches(document, json("'c.a': 4"))).isFalse();
        assertThat(matcher.matches(document, json("'c.a.x': 1"))).isFalse();

        document = json("{'array': [{'123a':{'name': 'old'}}]}");
        BSONObject query = json("{'array.123a.name': 'old'}");
        assertThat(matcher.matches(document, query)).isTrue();
    }

    @Test
    public void testMatchesSubqueryListPosition() throws Exception {
        BSONObject document = json("c: {a: [1,2,3] }");
        assertThat(matcher.matches(document, json("{}"))).isTrue();
        assertThat(matcher.matches(document, json("c: 1"))).isFalse();
        assertThat(matcher.matches(document, json("'c.a.0': 1"))).isTrue();
        assertThat(matcher.matches(document, json("'c.a.0': 2"))).isFalse();

        document = json("c: [{a: 12}, {a:13}]");
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
        BSONObject document = json("{}");
        BSONObject query = json("field: {$someInvalidOperator: 123}");
        try {
            matcher.matches(document, query);
            fail("MongoServerError expected");
        } catch (MongoServerError e) {
            assertThat(e.getCode()).isEqualTo(10068);
            assertThat(e.getMessage()).isEqualTo("invalid operator: $someInvalidOperator");
        }
    }

    @Test
    public void testMatchesExists() throws Exception {
        BSONObject document = json("{}");
        BSONObject query = json("qty: { $exists: true, $nin: [ 5, 15 ] }");

        assertThat(matcher.matches(document, query)).isFalse();

        document.put("qty", 17);
        assertThat(matcher.matches(document, query)).isTrue();

        document.put("qty", 15);
        assertThat(matcher.matches(document, query)).isFalse();
    }

    @Test
    public void testMatchesNotEqual() throws Exception {
        BSONObject document = json("{}");

        BSONObject query = json("qty: {$ne: 17}");

        assertThat(matcher.matches(document, query)).isTrue();

        document.put("qty", 17);
        assertThat(matcher.matches(document, query)).isFalse();

        document.put("qty", 15);
        assertThat(matcher.matches(document, query)).isTrue();
    }

    // http://docs.mongodb.org/v3.0/reference/operator/query/eq/#op._S_eq
    @Test
    public void testMatchesEqual() throws Exception {
        BSONObject document1 = json("_id: 1, item: { name: \"ab\", code: \"123\" }, qty: 15, tags: [ \"A\", \"B\", \"C\" ]");
        BSONObject document2 = json("_id: 2, item: { name: \"cd\", code: \"123\" }, qty: 20, tags: [ \"B\" ]");
        BSONObject document3 = json("_id: 3, item: { name: \"ij\", code: \"456\" }, qty: 25, tags: [ \"A\", \"B\" ]");
        BSONObject document4 = json("_id: 4, item: { name: \"xy\", code: \"456\" }, qty: 30, tags: [ \"B\", \"A\" ]");
        BSONObject document5 = json("_id: 5, item: { name: \"mn\", code: \"000\" }, qty: 20, tags: [ [ \"A\", \"B\" ], \"C\" ]");

        BSONObject query = json("qty: { $eq: 20 }");

        assertThat(matcher.matches(document1, query)).isFalse();
        assertThat(matcher.matches(document2, query)).isTrue();
        assertThat(matcher.matches(document3, query)).isFalse();
        assertThat(matcher.matches(document4, query)).isFalse();
        assertThat(matcher.matches(document5, query)).isTrue();
    }

    // http://docs.mongodb.org/v3.0/reference/operator/query/eq/#op._S_eq
    @Test
    public void testMatchesEqualEmbeddedDocument() throws Exception {
        BSONObject document1 = json("_id: 1, item: { name: \"ab\", code: \"123\" }, qty: 15, tags: [ \"A\", \"B\", \"C\" ]");
        BSONObject document2 = json("_id: 2, item: { name: \"cd\", code: \"123\" }, qty: 20, tags: [ \"B\" ]");
        BSONObject document3 = json("_id: 3, item: { name: \"ij\", code: \"456\" }, qty: 25, tags: [ \"A\", \"B\" ]");
        BSONObject document4 = json("_id: 4, item: { name: \"xy\", code: \"456\" }, qty: 30, tags: [ \"B\", \"A\" ]");
        BSONObject document5 = json("_id: 5, item: { name: \"mn\", code: \"000\" }, qty: 20, tags: [ [ \"A\", \"B\" ], \"C\" ]");

        BSONObject query = json("\"item.name\": { $eq: \"ab\" }");

        assertThat(matcher.matches(document1, query)).isTrue();
        assertThat(matcher.matches(document2, query)).isFalse();
        assertThat(matcher.matches(document3, query)).isFalse();
        assertThat(matcher.matches(document4, query)).isFalse();
        assertThat(matcher.matches(document5, query)).isFalse();
    }

    // http://docs.mongodb.org/v3.0/reference/operator/query/eq/#op._S_eq
    @Test
    public void testMatchesEqualOneArrayValue() throws Exception {
        BSONObject document1 = json("_id: 1, item: { name: \"ab\", code: \"123\" }, qty: 15, tags: [ \"A\", \"B\", \"C\" ]");
        BSONObject document2 = json("_id: 2, item: { name: \"cd\", code: \"123\" }, qty: 20, tags: [ \"B\" ]");
        BSONObject document3 = json("_id: 3, item: { name: \"ij\", code: \"456\" }, qty: 25, tags: [ \"A\", \"B\" ]");
        BSONObject document4 = json("_id: 4, item: { name: \"xy\", code: \"456\" }, qty: 30, tags: [ \"B\", \"A\" ]");
        BSONObject document5 = json("_id: 5, item: { name: \"mn\", code: \"000\" }, qty: 20, tags: [ [ \"A\", \"B\" ], \"C\" ]");

        BSONObject query = json("tags: { $eq: \"B\" }");

        assertThat(matcher.matches(document1, query)).isTrue();
        assertThat(matcher.matches(document2, query)).isTrue();
        assertThat(matcher.matches(document3, query)).isTrue();
        assertThat(matcher.matches(document4, query)).isTrue();
        assertThat(matcher.matches(document5, query)).isFalse();
    }

    // http://docs.mongodb.org/v3.0/reference/operator/query/eq/#op._S_eq
    @Test
    public void testMatchesEqualTwoArrayValues() throws Exception {
        BSONObject document1 = json("_id: 1, item: { name: \"ab\", code: \"123\" }, qty: 15, tags: [ \"A\", \"B\", \"C\" ]");
        BSONObject document2 = json("_id: 2, item: { name: \"cd\", code: \"123\" }, qty: 20, tags: [ \"B\" ]");
        BSONObject document3 = json("_id: 3, item: { name: \"ij\", code: \"456\" }, qty: 25, tags: [ \"A\", \"B\" ]");
        BSONObject document4 = json("_id: 4, item: { name: \"xy\", code: \"456\" }, qty: 30, tags: [ \"B\", \"A\" ]");
        BSONObject document5 = json("_id: 5, item: { name: \"mn\", code: \"000\" }, qty: 20, tags: [ [ \"A\", \"B\" ], \"C\" ]");

        BSONObject query = json("tags: { $eq: [ \"A\", \"B\" ] }");

        assertThat(matcher.matches(document1, query)).isFalse();
        assertThat(matcher.matches(document2, query)).isFalse();
        assertThat(matcher.matches(document3, query)).isTrue();
        assertThat(matcher.matches(document4, query)).isFalse();
        assertThat(matcher.matches(document5, query)).isTrue();
    }

    @Test
    public void testMatchesNot() throws Exception {

        // db.inventory.find( { } )
        BSONObject query = json("price: { $not: { $gt: 1.99 } }");

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

        BSONObject document = json("{}");
        assertThat(matcher.matches(document, query)).isTrue();

        document.put("price", 1.99);
        assertThat(matcher.matches(document, query)).isTrue();

        document.put("price", 1.990001);
        assertThat(matcher.matches(document, query)).isFalse();

        // !(x >= 5 && x <= 7)
        query = json("price: {$not: {$gte: 5, $lte: 7} }");
        assertThat(matcher.matches(document, query)).isTrue();
        document.put("price", 5);
        assertThat(matcher.matches(document, query)).isFalse();
        document.put("price", 7);
        assertThat(matcher.matches(document, query)).isFalse();
        document.put("price", null);
        assertThat(matcher.matches(document, query)).isTrue();

        query = json("price: {$not: {$exists: true}}");
        assertThat(matcher.matches(document, query)).isFalse();
        document.removeField("price");
        assertThat(matcher.matches(document, query)).isTrue();
    }

    /**
     * Test case for https://github.com/bwaldvogel/mongo-java-server/issues/7
     */
    @Test
    public void testMatchesNotIn() throws Exception {
        BSONObject query1 = json("{ \"map.key2\" : { \"$nin\" : [\"value 2.2\"] }}");
        BSONObject query2 = json("{ \"map.key2\" : { \"$not\" : {\"$in\" : [\"value 2.2\"] }}}");
        BSONObject query3 = json("{ \"map.key2\" : { \"$not\" : {\"$nin\" : [\"value 2.2\"] }}}");
        BSONObject query4 = json("{ \"map.key2\" : { \"$not\" : {\"$not\" : {\"$in\" : [\"value 2.2\"] }}}}");

        BSONObject document1 = json("{ \"code\" : \"c1\", \"map\" : { \"key1\" : [ \"value 1.1\" ], \"key2\" : [ \"value 2.1\" ] } }");
        BSONObject document2 = json("{ \"code\" : \"c1\", \"map\" : { \"key1\" : [ \"value 1.2\" ], \"key2\" : [ \"value 2.2\" ] } }");
        BSONObject document3 = json("{ \"code\" : \"c1\", \"map\" : { \"key1\" : [ \"value 2.1\" ], \"key2\" : [ \"value 2.1\" ] } }");

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
    }

    @Test
    public void testMatchesNotPattern() throws Exception {

        // { item: { $not: /^p.*/ } }
        BSONObject query = new BasicBSONObject("item", new BasicBSONObject("$not", Pattern.compile("^p.*")));

        BSONObject document = json("{}");
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

        BSONObject query = json("$and: [ { price: 1.99 }, { qty: { $lt: 20 } }, { sale: true } ]");

        /*
         * This query will select all documents in the inventory collection
         * where:
         *
         * price field value equals 1.99 and qty field value is less than 20 and
         * sale field value is equal to true.
         */

        BSONObject document = json("{}");
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

        BSONObject query = json("$or: [ { price: 1.99 }, { qty: { $lt: 20 } } ]");

        BSONObject document = json("{}");
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

        BSONObject query = json("$nor: [ { price: 1.99 }, { qty: { $lt: 20 } } ]");

        BSONObject document = json("{}");
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

        BSONObject document = json("{}");

        for (String op : new String[] { "$and", "$or", "$nor" }) {

            BSONObject query = new BasicBSONObject(op, null);
            try {
                matcher.matches(document, query);
                fail("MongoServerError expected");
            } catch (MongoServerError e) {
                assertThat(e.getCode()).isEqualTo(14816);
                assertThat(e.getMessage()).isEqualTo(op + " expression must be a nonempty array");
            }

            query.put(op, 2);
            try {
                matcher.matches(document, query);
                fail("MongoServerError expected");
            } catch (MongoServerError e) {
                assertThat(e.getCode()).isEqualTo(14816);
                assertThat(e.getMessage()).isEqualTo(op + " expression must be a nonempty array");
            }

            query.put(op, new ArrayList<Object>());
            try {
                matcher.matches(document, query);
                fail("MongoServerError expected");
            } catch (MongoServerError e) {
                assertThat(e.getCode()).isEqualTo(14816);
                assertThat(e.getMessage()).isEqualTo(op + " expression must be a nonempty array");
            }

            query.put(op, Arrays.asList("a"));
            try {
                matcher.matches(document, query);
                fail("MongoServerError expected");
            } catch (MongoServerError e) {
                assertThat(e.getCode()).isEqualTo(14817);
                assertThat(e.getMessage()).isEqualTo(op + " elements must be objects");
            }
        }
    }

    @Test
    public void testMatchesMod() throws Exception {
        BSONObject document = json("{}");

        BSONObject modOp = json("$mod: [4, 0]");
        BSONObject query = new BasicDBObject("x", modOp);
        assertThat(matcher.matches(document, query)).isFalse();

        for (int m = 0; m < 4; m++) {
            modOp.put("$mod", Arrays.asList(4, m));
            for (int i = 0; i < 20; i++) {
                document.put("x", i);
                assertThat(matcher.matches(document, query)).isEqualTo((i % 4) == m);
            }
        }
    }

    @Test
    public void testMatchesSize() throws Exception {

        BSONObject query = json("a: {$size: 1}");

        assertThat(matcher.matches(json("{}"), query)).isFalse();
        assertThat(matcher.matches(json("a : \"x\""), query)).isFalse();
        assertThat(matcher.matches(json("a:[]"), query)).isFalse();
        assertThat(matcher.matches(json("a:[1]"), query)).isTrue();
        assertThat(matcher.matches(json("a:[1,2]"), query)).isFalse();

    }

    @Test
    public void testMatchesAll() throws Exception {

        BSONObject document = json("{a: {x: []}}");

        assertThat(matcher.matches(document, json("x:{$all:[1]}"))).isFalse();

        assertThat(matcher.matches(document, json("a.x: {$all: []}"))).isTrue();
        assertThat(matcher.matches(document, json("a.x: {$all: [1]}"))).isFalse();

        document = json("{a: {x: [1,2,3]}}");
        assertThat(matcher.matches(document, json("a: {$all: [1]}"))).isFalse();
        assertThat(matcher.matches(document, json("a.x: {$all: []}"))).isTrue();
        assertThat(matcher.matches(document, json("a.x: {$all: [1]}"))).isTrue();
        assertThat(matcher.matches(document, json("a.y: {$all: [1]}"))).isFalse();
        assertThat(matcher.matches(document, json("a.x: {$all: [2]}"))).isTrue();
        assertThat(matcher.matches(document, json("a.x: {$all: [3]}"))).isTrue();
        assertThat(matcher.matches(document, json("a.x: {$all: [1,2,3]}"))).isTrue();
        assertThat(matcher.matches(document, json("a.x: {$all: [2,3]}"))).isTrue();
        assertThat(matcher.matches(document, json("a.x: {$all: [1,3]}"))).isTrue();
        assertThat(matcher.matches(document, json("a.x: {$all: [1,4]}"))).isFalse();
        assertThat(matcher.matches(document, json("a.x: {$all: [4]}"))).isFalse();

        // with regular expresssion
        document = json("a: {x: ['john', 'jo', 'maria']}");
        assertThat(matcher.matches(document, json("a.x: {$all: []}"))).isTrue();
        assertThat(matcher.matches(document, json("a.x: {$all: [{$regex: '^jo.*'}]}"))).isTrue();
        assertThat(matcher.matches(document, json("a.x: {$all: [{$regex: '^foo'}]}"))).isFalse();
        assertThat(matcher.matches(document, json("a.x: {$all: ['maria', {$regex: '^jo.*'}]}"))).isTrue();
    }

    @Test
    public void testMatchesAllSubdocument() throws Exception {
        // with subdocuments
        DBObject document = json("a: [{x: 1} , {x: 2}]");
        assertThat(matcher.matches(document, json("a.x: {$all : []}"))).isTrue();
        assertThat(matcher.matches(document, json("a.x: {$all : [1]}"))).isTrue();
        assertThat(matcher.matches(document, json("a.x: {$all : [2]}"))).isTrue();
        assertThat(matcher.matches(document, json("a.x: {$all : [1, 2]}"))).isTrue();
        assertThat(matcher.matches(document, json("a.x: {$all : [2, 3]}"))).isFalse();
        assertThat(matcher.matches(document, json("a.x: {$all : [3]}"))).isFalse();
    }

    @Test
    public void testMatchesAllAndIn() throws Exception {
        DBObject document = json("a: {x: [1, 3]}");
        assertThat(matcher.matches(document, json("a.x: {$all: [1,3], $in: [2]}"))).isFalse();
        assertThat(matcher.matches(document, json("a.x: {$all: [1,3], $in: [3]}"))).isTrue();

        document = json("a: {x: [1, 2, 3]}");
        assertThat(matcher.matches(document, json("a.x: {$all: [1,3], $in: [2]}"))).isTrue();

        document = json("a: [{x:1}, {x:2}, {x:3}]");
        assertThat(matcher.matches(document, json("a.x: {$all: [1,3], $in: [2]}"))).isTrue();
    }

}
