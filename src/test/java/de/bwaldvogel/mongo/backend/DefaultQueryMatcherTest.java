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

}
