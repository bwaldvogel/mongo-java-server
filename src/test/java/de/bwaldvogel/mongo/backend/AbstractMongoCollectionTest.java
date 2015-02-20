package de.bwaldvogel.mongo.backend;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Arrays;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.BasicDBObject;

import de.bwaldvogel.mongo.exception.MongoServerException;

public class AbstractMongoCollectionTest {

    private static class TestCollection extends AbstractMongoCollection<Position> {

        protected TestCollection(String databaseName, String collectionName, String idField) {
            super(databaseName, collectionName, idField);
        }

        @Override
        public void addDocument(BSONObject document) throws MongoServerException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int count() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected BSONObject getDocumentAt(Position position) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Iterable<DocumentWithPosition> iterateAllDocuments() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void removeDocumentAt(Position position) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Position scanDocumentPosition(BSONObject document) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected int getRecordCount() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected int getDeletedCount() {
            throw new UnsupportedOperationException();
        }

    }

    private TestCollection collection;

    @Before
    public void setUp() {
        this.collection = new TestCollection("some database", "some collection", "_id");
    }

    @Test
    public void testConvertSelector() throws Exception {
        BasicBSONObject selector = new BasicBSONObject();

        assertThat(collection.convertSelectorToDocument(selector)) //
                .isEqualTo(new BasicBSONObject());

        selector = new BasicBSONObject("_id", 1);
        assertThat(collection.convertSelectorToDocument(selector)) //
                .isEqualTo(new BasicBSONObject("_id", 1));

        selector = new BasicBSONObject("_id", 1).append("$set", new BasicBSONObject("foo", "bar"));
        assertThat(collection.convertSelectorToDocument(selector)) //
                .isEqualTo(new BasicBSONObject("_id", 1));

        selector = new BasicBSONObject("_id", 1).append("e.i", 14);
        assertThat(collection.convertSelectorToDocument(selector)) //
                .isEqualTo(new BasicBSONObject("_id", 1).append("e", new BasicBSONObject("i", 14)));

        selector = new BasicBSONObject("_id", 1).append("e.i.y", new BasicBSONObject("foo", "bar"));
        assertThat(collection.convertSelectorToDocument(selector)) //
                .isEqualTo(new BasicBSONObject("_id", 1).append("e", //
                        new BasicBSONObject("i", new BasicBSONObject("y", //
                                new BasicBSONObject("foo", "bar")))));
    }

    @Test
    public void testDeriveDocumentId() throws Exception {
        assertThat(collection.deriveDocumentId(new BasicBSONObject())).isInstanceOf(ObjectId.class);

        assertThat(collection.deriveDocumentId(new BasicBSONObject("a", 1))) //
                .isInstanceOf(ObjectId.class);

        assertThat(collection.deriveDocumentId(new BasicBSONObject("_id", 1))) //
                .isEqualTo(1);

        assertThat(collection.deriveDocumentId(new BasicBSONObject("_id", new BasicDBObject("$in", Arrays.asList(1))))) //
                .isEqualTo(1);

        assertThat(collection.deriveDocumentId(new BasicBSONObject("_id", new BasicDBObject("$in", Arrays.asList())))) //
                .isInstanceOf(ObjectId.class);
    }
}
