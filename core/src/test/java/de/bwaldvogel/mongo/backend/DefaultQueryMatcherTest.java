package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.backend.DocumentBuilder.allOf;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.and;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.elemMatch;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.eq;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.exists;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.gt;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.gte;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.in;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.list;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.lt;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.lte;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.map;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.mod;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.ne;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.nor;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.not;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.notIn;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.or;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.regex;
import static de.bwaldvogel.mongo.backend.DocumentBuilder.size;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import org.junit.Test;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class DefaultQueryMatcherTest {

    private QueryMatcher matcher = new DefaultQueryMatcher();

    @Test
    public void testMatchesSimple() throws Exception {
        Document document = map();
        assertThat(matcher.matches(document, map())).isTrue();
        assertThat(matcher.matches(document, map("foo", "bar"))).isFalse();
        document.put("foo", "bar");
        assertThat(matcher.matches(document, map("foo", "bar"))).isTrue();
    }

    @Test
    public void testMatchesPattern() throws Exception {
        Document document = map("name", "john");
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
        assertThat(matcher.matches(map(), query)).isFalse();
        assertThat(matcher.matches(map("a", "x"), query)).isFalse();
        assertThat(matcher.matches(map("a", 1), query)).isTrue();
        assertThat(matcher.matches(map("a", 2), query)).isTrue();
        assertThat(matcher.matches(map("a", 4), query)).isFalse();
        assertThat(matcher.matches(map("a", 1.0), query)).isTrue();
        Document otherQuery = map("a", in(3.0, 2.0, 1.00001));
        assertThat(matcher.matches(map("a", 1), otherQuery)).isFalse();
    }

    @Test
    public void testMatchesGreaterThanQuery() throws Exception {
        assertThat(matcher.matches(map(), map("a", gt(-1)))).isFalse();
        assertThat(matcher.matches(map("a", 1), map("a", gt(0.9)))).isTrue();
        assertThat(matcher.matches(map("a", 1), map("a", gt(0)))).isTrue();
        assertThat(matcher.matches(map("a", 1), map("a", gt(1)))).isFalse();
        assertThat(matcher.matches(map("a", 1), map("a", gte(1)))).isTrue();
        assertThat(matcher.matches(map("a", "x"), map("a", gt(1)))).isFalse();
        assertThat(matcher.matches(map("a", "x"), map("a", gte(1)))).isFalse();
    }

    @Test
    public void testMatchesLessThanQuery() throws Exception {
        assertThat(matcher.matches(map(), map("a", lt(-1)))).isFalse();
        assertThat(matcher.matches(map("a", 1), map("a", lt(1.001)))).isTrue();
        assertThat(matcher.matches(map("a", 1), map("a", lt(2)))).isTrue();
        assertThat(matcher.matches(map("a", 1), map("a", lt(1)))).isFalse();
        assertThat(matcher.matches(map("a", 1), map("a", lte(1)))).isTrue();
        assertThat(matcher.matches(map("a", "x"), map("a", lt(1)))).isFalse();
        assertThat(matcher.matches(map("a", "x"), map("a", lte(1)))).isFalse();
    }

    @Test
    public void testMatchesValueList() throws Exception {
        Document document = map("a", list(1, 2, 3));
        assertThat(matcher.matches(document, map())).isTrue();
        assertThat(matcher.matches(document, map("a", 1))).isTrue();
        assertThat(matcher.matches(document, map("a", 2))).isTrue();
        assertThat(matcher.matches(document, map("a", 3))).isTrue();
        assertThat(matcher.matches(document, map("a", 4))).isFalse();
    }

    @Test
    public void testMatchesDocumentList() throws Exception {
        Document document = map("_id", 1)
            .append("c", list(
                map("a", 1).append("b", 2),
                map("a", 3).append("b", 4))
        );

        assertThat(matcher.matches(document, map())).isTrue();
        assertThat(matcher.matches(document, map("c", 1))).isFalse();
        assertThat(matcher.matches(document, map("c.a", 1))).isTrue();
        assertThat(matcher.matches(document, map("c.a", 3))).isTrue();

        assertThat(matcher.matches(document, map("c.a", 1).append("c.b", 4))).isTrue();
        assertThat(matcher.matches(document, map("c.a", 1).append("c.b", 5))).isFalse();
        assertThat(matcher.matches(document, map("c.a", 2).append("c.b", 4))).isFalse();
    }

    @Test
    public void testMatchesSubquery() throws Exception {
        Document document = map("c", map("a", 1));
        assertThat(matcher.matches(document, map())).isTrue();
        assertThat(matcher.matches(document, map("c", 1))).isFalse();
        assertThat(matcher.matches(document, map("c.a", 1))).isTrue();
        assertThat(matcher.matches(document, map("c.a", 2))).isFalse();
        assertThat(matcher.matches(document, map("c.a.x", 2))).isFalse();

        document.put("a", map("b", map("c", map("d", 1))));
        assertThat(matcher.matches(document, map("a.b.c.d", 1))).isTrue();
        assertThat(matcher.matches(document, map("a.b", 1))).isFalse();
        assertThat(matcher.matches(document, map("a.b.c", map("d", 1)))).isTrue();
    }

    @Test
    public void testMatchesSubqueryList() throws Exception {
        Document document = map("c", map("a", list(1, 2, 3)));
        assertThat(matcher.matches(document, map())).isTrue();

        assertThat(matcher.matches(document, map("c", 1))).isFalse();
        assertThat(matcher.matches(document, map("c.a", 1))).isTrue();
        assertThat(matcher.matches(document, map("c.a", 2))).isTrue();
        assertThat(matcher.matches(document, map("c.a", 3))).isTrue();
        assertThat(matcher.matches(document, map("c.a", 4))).isFalse();
        assertThat(matcher.matches(document, map("c.a.x", 1))).isFalse();

        document = map("array", list(map("123a", map("name", "old"))));
        Document query = map("array.123a.name", "old");
        assertThat(matcher.matches(document, query)).isTrue();
    }

    @Test
    public void testMatchesSubqueryListPosition() throws Exception {
        Document document = map("c", map("a", list(1, 2, 3)));
        assertThat(matcher.matches(document, map())).isTrue();
        assertThat(matcher.matches(document, map("c", 1))).isFalse();
        assertThat(matcher.matches(document, map("c.a.0", 1))).isTrue();
        assertThat(matcher.matches(document, map("c.a.0", 2))).isFalse();

        document = map("c", list(map("a", 12), map("a", 13)));
        assertThat(matcher.matches(document, map("c.a", 12))).isTrue();
        assertThat(matcher.matches(document, map("c.a", 13))).isTrue();
        assertThat(matcher.matches(document, map("c.a", 14))).isFalse();
        assertThat(matcher.matches(document, map("c.0.a", 12))).isTrue();
        assertThat(matcher.matches(document, map("c.0.a", 13))).isFalse();
        assertThat(matcher.matches(document, map("c.1.a", 12))).isFalse();
        assertThat(matcher.matches(document, map("c.1.a", 13))).isTrue();
        assertThat(matcher.matches(document, map("c.2.a", 13))).isFalse();
    }

    @Test
    public void testInvalidOperator() throws Exception {
        Document document = map();
        Document query = map("field", map("$someInvalidOperator", 123));
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
        Document document = map();
        Document query = map("qty", exists().appendAll(notIn(5, 15)));

        assertThat(matcher.matches(document, query)).isFalse();

        document.put("qty", 17);
        assertThat(matcher.matches(document, query)).isTrue();

        document.put("qty", 15);
        assertThat(matcher.matches(document, query)).isFalse();
    }

    @Test
    public void testMatchesNotEqual() throws Exception {
        Document document = map();

        Document query = map("qty", ne(17));

        assertThat(matcher.matches(document, query)).isTrue();

        document.put("qty", 17);
        assertThat(matcher.matches(document, query)).isFalse();

        document.put("qty", 15);
        assertThat(matcher.matches(document, query)).isTrue();
    }

    // http://docs.mongodb.org/v3.0/reference/operator/query/eq/#op._S_eq
    @Test
    public void testMatchesEqual() throws Exception {
        Document document1 = map("_id", 1).append("item", map("name", "ab").append("code", "123")).append("qty", 15).append("tags", list("A", "B", "C"));
        Document document2 = map("_id", 2).append("item", map("name", "cd").append("code", "123")).append("qty", 20).append("tags", list("B"));
        Document document3 = map("_id", 3).append("item", map("name", "ij").append("code", "456")).append("qty", 25).append("tags", list("A", "B"));
        Document document4 = map("_id", 4).append("item", map("name", "xy").append("code", "456")).append("qty", 30).append("tags", list("B", "A"));
        Document document5 = map("_id", 5).append("item", map("name", "mn").append("code", "000")).append("qty", 20).append("tags", list(list("A", "B"), "C"));

        Document query = map("qty", eq(20));

        assertThat(matcher.matches(document1, query)).isFalse();
        assertThat(matcher.matches(document2, query)).isTrue();
        assertThat(matcher.matches(document3, query)).isFalse();
        assertThat(matcher.matches(document4, query)).isFalse();
        assertThat(matcher.matches(document5, query)).isTrue();
    }

    // http://docs.mongodb.org/v3.0/reference/operator/query/eq/#op._S_eq
    @Test
    public void testMatchesEqualEmbeddedDocument() throws Exception {
        Document document1 = map("_id", 1).append("item", map("name", "ab").append("code", "123")).append("qty", 15).append("tags", list("A", "B", "C"));
        Document document2 = map("_id", 2).append("item", map("name", "cd").append("code", "123")).append("qty", 20).append("tags", list("B"));
        Document document3 = map("_id", 3).append("item", map("name", "ij").append("code", "456")).append("qty", 25).append("tags", list("A", "B"));
        Document document4 = map("_id", 4).append("item", map("name", "xy").append("code", "456")).append("qty", 30).append("tags", list("B", "A"));
        Document document5 = map("_id", 5).append("item", map("name", "mn").append("code", "000")).append("qty", 20).append("tags", list(list("A", "B"), "C"));

        Document query = map("item.name", eq("ab"));

        assertThat(matcher.matches(document1, query)).isTrue();
        assertThat(matcher.matches(document2, query)).isFalse();
        assertThat(matcher.matches(document3, query)).isFalse();
        assertThat(matcher.matches(document4, query)).isFalse();
        assertThat(matcher.matches(document5, query)).isFalse();
    }

    // http://docs.mongodb.org/v3.0/reference/operator/query/eq/#op._S_eq
    @Test
    public void testMatchesEqualOneArrayValue() throws Exception {
        Document document1 = map("_id", 1).append("item", map("name", "ab").append("code", "123")).append("qty", 15).append("tags", list("A", "B", "C"));
        Document document2 = map("_id", 2).append("item", map("name", "cd").append("code", "123")).append("qty", 20).append("tags", list("B"));
        Document document3 = map("_id", 3).append("item", map("name", "ij").append("code", "456")).append("qty", 25).append("tags", list("A", "B"));
        Document document4 = map("_id", 4).append("item", map("name", "xy").append("code", "456")).append("qty", 30).append("tags", list("B", "A"));
        Document document5 = map("_id", 5).append("item", map("name", "mn").append("code", "000")).append("qty", 20).append("tags", list(list("A", "B"), "C"));

        Document query = map("tags", eq("B"));

        assertThat(matcher.matches(document1, query)).isTrue();
        assertThat(matcher.matches(document2, query)).isTrue();
        assertThat(matcher.matches(document3, query)).isTrue();
        assertThat(matcher.matches(document4, query)).isTrue();
        assertThat(matcher.matches(document5, query)).isFalse();
    }

    // http://docs.mongodb.org/v3.0/reference/operator/query/eq/#op._S_eq
    @Test
    public void testMatchesEqualTwoArrayValues() throws Exception {
        Document document1 = map("_id", 1).append("item", map("name", "ab").append("code", "123")).append("qty", 15).append("tags", list("A", "B", "C"));
        Document document2 = map("_id", 2).append("item", map("name", "cd").append("code", "123")).append("qty", 20).append("tags", list("B"));
        Document document3 = map("_id", 3).append("item", map("name", "ij").append("code", "456")).append("qty", 25).append("tags", list("A", "B"));
        Document document4 = map("_id", 4).append("item", map("name", "xy").append("code", "456")).append("qty", 30).append("tags", list("B", "A"));
        Document document5 = map("_id", 5).append("item", map("name", "mn").append("code", "000")).append("qty", 20).append("tags", list(list("A", "B"), "C"));

        Document query = map("tags", eq(list("A", "B")));

        assertThat(matcher.matches(document1, query)).isFalse();
        assertThat(matcher.matches(document2, query)).isFalse();
        assertThat(matcher.matches(document3, query)).isTrue();
        assertThat(matcher.matches(document4, query)).isFalse();
        assertThat(matcher.matches(document5, query)).isTrue();
    }

    @Test
    public void testMatchesNot() throws Exception {

        // db.inventory.find( { } )
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

        Document document = map();
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

        Document document1 = map("code", "c1").append("map", map("key1", "value 1.1").append("key2", list("value 2.1")));
        Document document2 = map("code", "c1").append("map", map("key1", "value 1.2").append("key2", list("value 2.2")));
        Document document3 = map("code", "c1").append("map", map("key1", "value 2.1").append("key2", list("value 2.1")));

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
        Document query = map("item", not(regex("^p.*")));

        Document document = map();
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

        Document query = and(map("price", 1.99), map("qty", lt(20)).append("sale", true));

        /*
         * This query will select all documents in the inventory collection
         * where:
         *
         * price field value equals 1.99 and qty field value is less than 20 and
         * sale field value is equal to true.
         */

        Document document = map();
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

        Document query = or(map("price", 1.99), map("qty", lt(20)));

        Document document = map();
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

        Document query = nor(map("price", 1.99), map("qty", lt(20)));

        Document document = map();
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
            assertNonEmptyArrayException(op, map(op.getValue(), null));
            assertNonEmptyArrayException(op, map(op.getValue(), 2));
            assertNonEmptyArrayException(op, map(op.getValue(), 2));

            try {
                matcher.matches(map(), map(op, "a"));
                fail("MongoServerError expected");
            } catch (MongoServerError e) {
                assertThat(e.getCode()).isEqualTo(14817);
                assertThat(e.getMessage()).isEqualTo(op + " elements must be objects");
            }
        }
    }

    private void assertNonEmptyArrayException(QueryFilter op, Document query) throws Exception {
        Document emptyDocument = map();
        try {
            matcher.matches(emptyDocument, query);
            fail("MongoServerError expected");
        } catch (MongoServerError e) {
            assertThat(e.getCode()).isEqualTo(14816);
            assertThat(e.getMessage()).isEqualTo(op.getValue() + " expression must be a nonempty array");
        }
    }

    @Test
    public void testMatchesMod() throws Exception {
        Document document = map();

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

        assertThat(matcher.matches(map(), query)).isFalse();
        assertThat(matcher.matches(map("a", "x"), query)).isFalse();
        assertThat(matcher.matches(map("a", list()), query)).isFalse();
        assertThat(matcher.matches(map("a", list(1)), query)).isTrue();
        assertThat(matcher.matches(map("a", list(1, 2)), query)).isFalse();

    }

    @Test
    public void testMatchesAll() throws Exception {

        Document document = map("a", map("x", list()));

        assertThat(matcher.matches(document, map("x", allOf(1)))).isFalse();

        assertThat(matcher.matches(document, map("a.x", allOf()))).isTrue();
        assertThat(matcher.matches(document, map("a.x", allOf(1)))).isFalse();

        document = map("a", map("x", list(1, 2, 3)));
        assertThat(matcher.matches(document, map("a", allOf(1)))).isFalse();
        assertThat(matcher.matches(document, map("a.x", allOf()))).isTrue();
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
        assertThat(matcher.matches(document, map("a.x", allOf()))).isTrue();
        assertThat(matcher.matches(document, map("a.x", allOf(regex("^jo.*"))))).isTrue();
        assertThat(matcher.matches(document, map("a.x", allOf(regex("^foo"))))).isFalse();
        assertThat(matcher.matches(document, map("a.x", allOf("maria", regex("^jo.*"))))).isTrue();
    }

    @Test
    public void testMatchesAllSubdocument() throws Exception {
        // with subdocuments
        Document document = map("a", list(map("x", 1), map("x", 2)));
        assertThat(matcher.matches(document, map("a.x", allOf()))).isTrue();
        assertThat(matcher.matches(document, map("a.x", allOf(1)))).isTrue();
        assertThat(matcher.matches(document, map("a.x", allOf(2)))).isTrue();
        assertThat(matcher.matches(document, map("a.x", allOf(1, 2)))).isTrue();
        assertThat(matcher.matches(document, map("a.x", allOf(2, 3)))).isFalse();
        assertThat(matcher.matches(document, map("a.x", allOf(3)))).isFalse();
    }

    @Test
    public void testMatchesAllAndIn() throws Exception {
        Document document = map("a", map("x", list(1, 3)));
        assertThat(matcher.matches(document, map("a.x", allOf(1, 3).appendAll(in(2))))).isFalse();
        assertThat(matcher.matches(document, map("a.x", allOf(1, 3).appendAll(in(3))))).isTrue();

        document = map("a", map("x", list(1, 2, 3)));
        assertThat(matcher.matches(document, map("a.x", allOf(1, 3).appendAll(in(2))))).isTrue();

        document = map("a", list(map("x", 1), map("x", 2), map("x", 3)));
        assertThat(matcher.matches(document, map("a.x", allOf(1, 3).appendAll(in(2))))).isTrue();
    }

    @Test
    public void testMatchesElement() throws Exception {
        Document document1 = map("results", list(82, 85, 88));
        assertThat(matcher.matches(document1, map("results", elemMatch(gte(80))))).isTrue();
        assertThat(matcher.matches(document1, map("results", elemMatch(gte(80).appendAll(lt(85)))))).isTrue();
        assertThat(matcher.matches(document1, map("results", elemMatch(gte(90).appendAll(lt(85)))))).isFalse();
        assertThat(matcher.matches(document1, map("results", elemMatch(gte(70).appendAll(lt(80)))))).isFalse();

        Document document2 = map("_id", 2).appendAll(map("results", list(75, 88, 89)));
        assertThat(matcher.matches(document2, map("results", elemMatch(gte(80).appendAll(lt(85)))))).isFalse();
    }

    @Test
    public void testMatchesElementInEmbeddedDocuments() throws Exception {
        Document document1 = map("_id", 1).append("results", list(map("product", "abc").append("score", 10), map("product", "xyz").append("score", 5)));
        Document document2 = map("_id", 2).append("results", list(map("product", "abc").append("score", 9), map("product", "xyz").append("score", 7)));
        Document document3 = map("_id", 3).append("results", list(map("product", "abc").append("score", 7), map("product", "xyz").append("score", 8)));

        Document query = map("results", elemMatch(map("product", "xyz").append("score", gte(8))));
        assertThat(matcher.matches(document1, query)).isFalse();
        assertThat(matcher.matches(document2, query)).isFalse();
        assertThat(matcher.matches(document3, query)).isTrue();

        Document hasProductXyzElement = map("results", elemMatch(map("product", "xyz")));
        assertThat(matcher.matches(document1, hasProductXyzElement)).isTrue();
        assertThat(matcher.matches(document2, hasProductXyzElement)).isTrue();
        assertThat(matcher.matches(document3, hasProductXyzElement)).isTrue();
    }

    @Test
    public void testEmptyMatchesElementQuery() throws Exception {
        Document document = map("_id", 1).append("results", list(map("product", "xyz").append("score", 5)));
        assertThat(matcher.matches(document, map("results", elemMatch(map())))).isTrue();
    }

}
