package de.bwaldvogel.mongo.backend;

import static org.fest.assertions.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.BasicDBObject;

public class DefaultQueryMatcherTest {

    private QueryMatcher matcher;

    @Before
    public void setUp() {
        matcher = new DefaultQueryMatcher();
    }

    @Test
    public void testMatchesSimple() {
        BSONObject document = new BasicDBObject();
        assertThat(matcher.matches(document, new BasicBSONObject())).isTrue();
        assertThat(matcher.matches(document, new BasicBSONObject("foo", "bar"))).isFalse();
        document.put("foo", "bar");
        assertThat(matcher.matches(document, new BasicBSONObject("foo", "bar"))).isTrue();
    }

    @Test
    public void testMatchesInQuery() {
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
    public void testMatchesValueList() {
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
    public void testMatchesSubquery() {
        BSONObject document = new BasicDBObject();
        document.put("c", new BasicDBObject("a", 1));
        assertThat(matcher.matches(document, new BasicBSONObject())).isTrue();
        assertThat(matcher.matches(document, new BasicBSONObject("c", 1))).isFalse();
        assertThat(matcher.matches(document, new BasicBSONObject("c.a", 1))).isTrue();
        assertThat(matcher.matches(document, new BasicBSONObject("c.a", 2))).isFalse();
    }

    @Test
    public void testMatchesSubqueryList() {
        BSONObject document = new BasicDBObject();
        document.put("c", new BasicDBObject("a", Arrays.asList(1, 2, 3)));
        assertThat(matcher.matches(document, new BasicBSONObject())).isTrue();
        assertThat(matcher.matches(document, new BasicBSONObject("c", 1))).isFalse();
        assertThat(matcher.matches(document, new BasicBSONObject("c.a", 1))).isTrue();
        assertThat(matcher.matches(document, new BasicBSONObject("c.a", 2))).isTrue();
        assertThat(matcher.matches(document, new BasicBSONObject("c.a", 3))).isTrue();
        assertThat(matcher.matches(document, new BasicBSONObject("c.a", 4))).isFalse();
    }

}
