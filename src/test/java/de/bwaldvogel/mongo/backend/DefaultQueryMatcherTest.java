package de.bwaldvogel.mongo.backend;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.BasicDBObject;

import de.bwaldvogel.mongo.exception.MongoServerError;

public class DefaultQueryMatcherTest {

    private QueryMatcher matcher;

    @Before
    public void setUp() {
        matcher = new DefaultQueryMatcher();
    }

    @Test
    public void testMatchesSimple() throws Exception {
        BSONObject document = new BasicDBObject();
        assertThat(matcher.matches(document, new BasicBSONObject())).isTrue();
        assertThat(matcher.matches(document, new BasicBSONObject("foo", "bar"))).isFalse();
        document.put("foo", "bar");
        assertThat(matcher.matches(document, new BasicBSONObject("foo", "bar"))).isTrue();
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
        assertThat(matcher.matches(document, new BasicBSONObject("name", Pattern.compile(name))));

        assertThat(matcher.matches(document, new BasicBSONObject("name", new BasicBSONObject("$regex", name))));
    }

    @Test
    public void testMatchesInQuery() throws Exception {
        assertThat(matcher.matches(new BasicDBObject(), //
                new BasicBSONObject("a", new BasicDBObject("$in", Arrays.asList(3, 2, 1))))) //
                .isFalse();

        assertThat(matcher.matches(new BasicDBObject("a", "x"), //
                new BasicBSONObject("a", new BasicDBObject("$in", Arrays.asList(3, 2, 1))))) //
                .isFalse();

        assertThat(matcher.matches(new BasicDBObject("a", 1), //
                new BasicBSONObject("a", new BasicDBObject("$in", Arrays.asList(3, 2, 1))))) //
                .isTrue();

        assertThat(matcher.matches(new BasicDBObject("a", 2), //
                new BasicBSONObject("a", new BasicDBObject("$in", Arrays.asList(3, 2, 1))))) //
                .isTrue();

        assertThat(matcher.matches(new BasicDBObject("a", 4), //
                new BasicBSONObject("a", new BasicDBObject("$in", Arrays.asList(3, 2, 1))))) //
                .isFalse();

        assertThat(matcher.matches(new BasicDBObject("a", 1.0), //
                new BasicBSONObject("a", new BasicDBObject("$in", Arrays.asList(3, 2, 1))))) //
                .isTrue();

        assertThat(matcher.matches(new BasicDBObject("a", 1), //
                new BasicBSONObject("a", new BasicDBObject("$in", Arrays.asList(3.0, 2.0, 1.00001))))) //
                .isFalse();
    }

    @Test
    public void testMatchesGreaterThanQuery() throws Exception {
        assertThat(matcher.matches(new BasicDBObject(), //
                new BasicBSONObject("a", new BasicDBObject("$gt", -1)))) //
                .isFalse();

        assertThat(matcher.matches(new BasicDBObject("a", 1), //
                new BasicBSONObject("a", new BasicDBObject("$gt", 0.9)))) //
                .isTrue();

        assertThat(matcher.matches(new BasicDBObject("a", 1), //
                new BasicBSONObject("a", new BasicDBObject("$gt", 0)))) //
                .isTrue();

        assertThat(matcher.matches(new BasicDBObject("a", 1), //
                new BasicBSONObject("a", new BasicDBObject("$gt", 1)))) //
                .isFalse();

        assertThat(matcher.matches(new BasicDBObject("a", 1), //
                new BasicBSONObject("a", new BasicDBObject("$gte", 1)))) //
                .isTrue();

        assertThat(matcher.matches(new BasicDBObject("a", "x"), //
                new BasicBSONObject("a", new BasicDBObject("$gt", 1)))) //
                .isFalse();

        assertThat(matcher.matches(new BasicDBObject("a", "x"), //
                new BasicBSONObject("a", new BasicDBObject("$gte", 1)))) //
                .isFalse();
    }

    @Test
    public void testMatchesLessThanQuery() throws Exception {
        assertThat(matcher.matches(new BasicDBObject(), //
                new BasicBSONObject("a", new BasicDBObject("$lt", -1)))) //
                .isFalse();

        assertThat(matcher.matches(new BasicDBObject("a", 1), //
                new BasicBSONObject("a", new BasicDBObject("$lt", 1.001)))) //
                .isTrue();

        assertThat(matcher.matches(new BasicDBObject("a", 1), //
                new BasicBSONObject("a", new BasicDBObject("$lt", 2)))) //
                .isTrue();

        assertThat(matcher.matches(new BasicDBObject("a", 1), //
                new BasicBSONObject("a", new BasicDBObject("$lt", 1)))) //
                .isFalse();

        assertThat(matcher.matches(new BasicDBObject("a", 1), //
                new BasicBSONObject("a", new BasicDBObject("$lte", 1)))) //
                .isTrue();

        assertThat(matcher.matches(new BasicDBObject("a", "x"), //
                new BasicBSONObject("a", new BasicDBObject("$lt", 1)))) //
                .isFalse();

        assertThat(matcher.matches(new BasicDBObject("a", "x"), //
                new BasicBSONObject("a", new BasicDBObject("$lte", 1)))) //
                .isFalse();
    }

    @Test
    public void testMatchesValueList() throws Exception {
        BSONObject document = new BasicDBObject();
        document.put("a", Arrays.asList(1, 2, 3));
        assertThat(matcher.matches(document, new BasicBSONObject())).isTrue();
        assertThat(matcher.matches(document, new BasicBSONObject("a", 1))).isTrue();
        assertThat(matcher.matches(document, new BasicBSONObject("a", 2))).isTrue();
        assertThat(matcher.matches(document, new BasicBSONObject("a", 3))).isTrue();
        assertThat(matcher.matches(document, new BasicBSONObject("a", 4))).isFalse();
    }

    @Test
    public void testMatchesDocumentList() throws Exception {
        // { "_id" : 1 , "c" : [ { "a" : 1 , "b" : 2} , { "a" : 3 , "b" : 4}]}
        BSONObject document = new BasicDBObject("_id", 1);
        List<BSONObject> list = new ArrayList<BSONObject>();
        list.add(new BasicBSONObject("a", 1).append("b", 2));
        list.add(new BasicBSONObject("a", 3).append("b", 4));
        document.put("c", list);

        assertThat(matcher.matches(document, new BasicBSONObject())).isTrue();
        assertThat(matcher.matches(document, new BasicBSONObject("c", 1))).isFalse();
        assertThat(matcher.matches(document, new BasicBSONObject("c.a", 1))).isTrue();
        assertThat(matcher.matches(document, new BasicBSONObject("c.a", 3))).isTrue();

        assertThat(matcher.matches(document, new BasicBSONObject("c.a", 1).append("c.b", 4))).isTrue();
        assertThat(matcher.matches(document, new BasicBSONObject("c.a", 1).append("c.b", 5))).isFalse();
        assertThat(matcher.matches(document, new BasicBSONObject("c.a", 2).append("c.b", 4))).isFalse();
    }

    @Test
    public void testMatchesSubquery() throws Exception {
        BSONObject document = new BasicDBObject();
        document.put("c", new BasicDBObject("a", 1));
        assertThat(matcher.matches(document, new BasicBSONObject())).isTrue();
        assertThat(matcher.matches(document, new BasicBSONObject("c", 1))).isFalse();
        assertThat(matcher.matches(document, new BasicBSONObject("c.a", 1))).isTrue();
        assertThat(matcher.matches(document, new BasicBSONObject("c.a", 2))).isFalse();
        assertThat(matcher.matches(document, new BasicBSONObject("c.a.x", 2))).isFalse();

        document.put("a", new BasicBSONObject("b", new BasicBSONObject("c", new BasicBSONObject("d", 1))));
        assertThat(matcher.matches(document, new BasicBSONObject("a.b.c.d", 1))).isTrue();
        assertThat(matcher.matches(document, new BasicBSONObject("a.b", 1))).isFalse();
        assertThat(matcher.matches(document, new BasicBSONObject("a.b.c", new BasicBSONObject("d", 1)))).isTrue();
    }

    @Test
    public void testMatchesSubqueryList() throws Exception {
        BSONObject document = new BasicDBObject();
        document.put("c", new BasicDBObject("a", Arrays.asList(1, 2, 3)));
        assertThat(matcher.matches(document, new BasicBSONObject())).isTrue();
        assertThat(matcher.matches(document, new BasicBSONObject("c", 1))).isFalse();
        assertThat(matcher.matches(document, new BasicBSONObject("c.a", 1))).isTrue();
        assertThat(matcher.matches(document, new BasicBSONObject("c.a", 2))).isTrue();
        assertThat(matcher.matches(document, new BasicBSONObject("c.a", 3))).isTrue();
        assertThat(matcher.matches(document, new BasicBSONObject("c.a", 4))).isFalse();
    }

    @Test
    public void testMatchesSubqueryListPosition() throws Exception {
        BSONObject document = new BasicDBObject();
        document.put("c", new BasicDBObject("a", Arrays.asList(1, 2, 3)));
        assertThat(matcher.matches(document, new BasicBSONObject())).isTrue();
        assertThat(matcher.matches(document, new BasicBSONObject("c", 1))).isFalse();
        assertThat(matcher.matches(document, new BasicBSONObject("c.a.0", 1))).isTrue();
        assertThat(matcher.matches(document, new BasicBSONObject("c.a.0", 2))).isFalse();

        document.put("c", Arrays.asList(new BasicDBObject("a", 12), new BasicDBObject("a", 13)));
        assertThat(matcher.matches(document, new BasicBSONObject("c.a", 12))).isTrue();
        assertThat(matcher.matches(document, new BasicBSONObject("c.a", 13))).isTrue();
        assertThat(matcher.matches(document, new BasicBSONObject("c.a", 14))).isFalse();
        assertThat(matcher.matches(document, new BasicBSONObject("c.0.a", 12))).isTrue();
        assertThat(matcher.matches(document, new BasicBSONObject("c.0.a", 13))).isFalse();
        assertThat(matcher.matches(document, new BasicBSONObject("c.1.a", 12))).isFalse();
        assertThat(matcher.matches(document, new BasicBSONObject("c.1.a", 13))).isTrue();
        assertThat(matcher.matches(document, new BasicBSONObject("c.2.a", 13))).isFalse();
    }

    @Test
    public void testInvalidOperator() throws Exception {
        BSONObject document = new BasicDBObject();
        BSONObject query = new BasicDBObject("field", new BasicDBObject("$someInvalidOperator", 123));
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
        BSONObject document = new BasicDBObject();

        // db.inventory.find( { qty: { $exists: true, $nin: [ 5, 15 ] } } )
        BSONObject query = new BasicBSONObject("qty", new BasicBSONObject("$exists", true).append("$nin",
                Arrays.asList(5, 15)));

        assertThat(matcher.matches(document, query)).isFalse();

        document.put("qty", 17);
        assertThat(matcher.matches(document, query)).isTrue();

        document.put("qty", 15);
        assertThat(matcher.matches(document, query)).isFalse();
    }

    @Test
    public void testMatchesNotEqual() throws Exception {
        BSONObject document = new BasicDBObject();

        BSONObject query = new BasicBSONObject("qty", new BasicBSONObject("$ne", 17));

        assertThat(matcher.matches(document, query)).isTrue();

        document.put("qty", 17);
        assertThat(matcher.matches(document, query)).isFalse();

        document.put("qty", 15);
        assertThat(matcher.matches(document, query)).isTrue();
    }

    @Test
    public void testMatchesNot() throws Exception {

        // db.inventory.find( { price: { $not: { $gt: 1.99 } } } )
        BSONObject query = new BasicBSONObject("price", new BasicBSONObject("$not", new BasicBSONObject("$gt", 1.99)));

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

        BSONObject document = new BasicDBObject();
        assertThat(matcher.matches(document, query)).isTrue();

        document.put("price", 1.99);
        assertThat(matcher.matches(document, query)).isTrue();

        document.put("price", 1.990001);
        assertThat(matcher.matches(document, query)).isFalse();

        // !(x >= 5 && x <= 7)
        query = new BasicBSONObject("price", new BasicBSONObject("$not", new BasicBSONObject("$gte", 5).append("$lte",
                7)));
        assertThat(matcher.matches(document, query)).isTrue();
        document.put("price", 5);
        assertThat(matcher.matches(document, query)).isFalse();
        document.put("price", 7);
        assertThat(matcher.matches(document, query)).isFalse();
        document.put("price", null);
        assertThat(matcher.matches(document, query)).isTrue();

        query = new BasicBSONObject("price", new BasicBSONObject("$not", new BasicBSONObject("$exists", true)));
        assertThat(matcher.matches(document, query)).isFalse();
        document.removeField("price");
        assertThat(matcher.matches(document, query)).isTrue();
    }

    @Test
    public void testMatchesNotPattern() throws Exception {

        // { item: { $not: /^p.*/ } }
        BSONObject query = new BasicBSONObject("item", new BasicBSONObject("$not", Pattern.compile("^p.*")));

        BSONObject document = new BasicDBObject();
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

        // { $and: [ { price: 1.99 }, { qty: { $lt: 20 } }, { sale: true } ] } )
        BSONObject query = new BasicBSONObject("$and", Arrays.asList(new BasicBSONObject("price", 1.99),
                new BasicBSONObject("qty", new BasicBSONObject("$lt", 20)), new BasicBSONObject("sale", true)));

        /*
         * This query will select all documents in the inventory collection
         * where:
         *
         * price field value equals 1.99 and qty field value is less than 20 and
         * sale field value is equal to true.
         */

        BSONObject document = new BasicDBObject();
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

        // { $or: [ { price: 1.99 }, { qty: { $lt: 20 } } ] } )
        BSONObject query = new BasicBSONObject("$or", Arrays.asList(new BasicBSONObject("price", 1.99),
                new BasicBSONObject("qty", new BasicBSONObject("$lt", 20))));

        BSONObject document = new BasicDBObject();
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

        // { $or: [ { price: 1.99 }, { qty: { $lt: 20 } } ] } )
        BSONObject query = new BasicBSONObject("$nor", Arrays.asList(new BasicBSONObject("price", 1.99),
                new BasicBSONObject("qty", new BasicBSONObject("$lt", 20))));

        BSONObject document = new BasicDBObject();
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

        BSONObject document = new BasicDBObject();

        for (String op : new String[] { "$and", "$or", "$nor" }) {

            BSONObject query = new BasicBSONObject(op, null);
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
        BSONObject document = new BasicDBObject();

        BSONObject modOp = new BasicDBObject("$mod", Arrays.asList(4, 0));
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

        BSONObject document = new BasicDBObject();
        BSONObject query = new BasicDBObject("a", new BasicDBObject("$size", 1));

        assertThat(matcher.matches(document, query)).isFalse();
        document.put("a", "x");
        assertThat(matcher.matches(document, query)).isFalse();
        document.put("a", Arrays.asList());
        assertThat(matcher.matches(document, query)).isFalse();
        document.put("a", Arrays.asList(1));
        assertThat(matcher.matches(document, query)).isTrue();
        document.put("a", Arrays.asList(1, 2));
        assertThat(matcher.matches(document, query)).isFalse();

    }
}
