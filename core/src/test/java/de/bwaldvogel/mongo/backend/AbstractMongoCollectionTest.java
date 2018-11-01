package de.bwaldvogel.mongo.backend;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.ObjectId;

public class AbstractMongoCollectionTest {

    private static class TestCollection extends AbstractMongoCollection<Object> {

        protected TestCollection(String databaseName, String collectionName, String idField) {
            super(databaseName, collectionName, idField);
        }

        @Override
        protected Object addDocumentInternal(Document document) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int count() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Document getDocument(Object position) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void removeDocument(Object position) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Object findDocumentPosition(Document document) {
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

        @Override
        protected Iterable<Document> matchDocuments(Document query, Iterable<Object> positions, Document orderBy,
                                                    int numberToSkip, int numberToReturn) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Iterable<Document> matchDocuments(Document query, Document orderBy, int numberToSkip,
                int numberToReturn) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void updateDataSize(long sizeDelta) {
        }

        @Override
        protected long getDataSize() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void handleUpdate(Document document) {
            // noop
        }
    }

    private TestCollection collection;

    @Before
    public void setUp() {
        this.collection = new TestCollection("some database", "some collection", "_id");
    }

    @Test
    public void testConvertSelector() throws Exception {
        Document selector = new Document();

        assertThat(collection.convertSelectorToDocument(selector)) //
                .isEqualTo(new Document());

        selector = new Document("_id", 1);
        assertThat(collection.convertSelectorToDocument(selector)) //
                .isEqualTo(new Document("_id", 1));

        selector = new Document("_id", 1).append("$set", new Document("foo", "bar"));
        assertThat(collection.convertSelectorToDocument(selector)) //
                .isEqualTo(new Document("_id", 1));

        selector = new Document("_id", 1).append("e.i", 14);
        assertThat(collection.convertSelectorToDocument(selector)) //
                .isEqualTo(new Document("_id", 1).append("e", new Document("i", 14)));

        selector = new Document("_id", 1).append("e.i.y", new Document("foo", "bar"));
        assertThat(collection.convertSelectorToDocument(selector)) //
                .isEqualTo(new Document("_id", 1).append("e", //
                        new Document("i", new Document("y", //
                                new Document("foo", "bar")))));
    }

    @Test
    public void testDeriveDocumentId() throws Exception {
        assertThat(collection.deriveDocumentId(new Document()))
            .isInstanceOf(ObjectId.class);

        assertThat(collection.deriveDocumentId(new Document("a", 1)))
            .isInstanceOf(ObjectId.class);

        assertThat(collection.deriveDocumentId(new Document("_id", 1)))
            .isEqualTo(1);

        assertThat(collection.deriveDocumentId(new Document("_id",
            new Document("$in", Collections.singletonList(1)))))
                .isEqualTo(1);

        assertThat(collection.deriveDocumentId(new Document("_id",
            new Document("$in", Collections.emptyList()))))
                .isInstanceOf(ObjectId.class);
    }
}
