package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThat;

import de.bwaldvogel.mongo.exception.ImmutableFieldException;
import org.assertj.core.util.Lists;
import org.assertj.core.util.Maps;
import org.assertj.core.util.Sets;
import org.junit.Before;
import org.junit.Test;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.ObjectId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AbstractMongoCollectionTest {

    private static class TestCollection extends AbstractMongoCollection<Object> {

        private final List<Document> existingDocuments;

        TestCollection(String databaseName, String collectionName, String idField) {
            this(databaseName, collectionName, idField, new ArrayList<>());
        }

        TestCollection(String databaseName, String collectionName, String idField, final List<Document> existingDocuments) {
            super(databaseName, collectionName, idField);
            this.existingDocuments = existingDocuments;
        }

        @Override
        protected Object addDocumentInternal(Document document) {
            final int size = existingDocuments.size();
            existingDocuments.add(document);

            return size;
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
        protected Iterable<Document> matchDocuments(Document query, Iterable<Object> positions, Document orderBy,
                                                    int numberToSkip, int numberToReturn) {
            throw new UnsupportedOperationException();
        }

        // todo: actually enforce all the parameters
        @Override
        protected Iterable<Document> matchDocuments(Document query, Document orderBy, int numberToSkip,
                                                    int numberToReturn) {
            return existingDocuments;
        }

        @Override
        protected void updateDataSize(int sizeDelta) {
        }

        @Override
        protected int getDataSize() {
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
        assertThat(collection.convertSelectorToDocument(json("")))
            .isEqualTo(json(""));

        assertThat(collection.convertSelectorToDocument(json("_id: 1")))
            .isEqualTo(json("_id: 1"));

        assertThat(collection.convertSelectorToDocument(json("_id: 1, $set: {foo: 'bar'}")))
            .isEqualTo(json("_id: 1"));

        assertThat(collection.convertSelectorToDocument(json("_id: 1, 'e.i': 14")))
            .isEqualTo(json("_id: 1, e: {i: 14}"));

        assertThat(collection.convertSelectorToDocument(json("_id: 1, 'e.i.y': {foo: 'bar'}")))
            .isEqualTo(json("_id: 1, e: {i: {y: {foo: 'bar'}}}"));
    }

    @Test
    public void testDeriveDocumentId() throws Exception {
        assertThat(collection.deriveDocumentId(json(""))).isInstanceOf(ObjectId.class);
        assertThat(collection.deriveDocumentId(json("a: 1"))).isInstanceOf(ObjectId.class);
        assertThat(collection.deriveDocumentId(json("_id: 1"))).isEqualTo(1);
        assertThat(collection.deriveDocumentId(json("_id: {$in: [1]}"))).isEqualTo(1);
        assertThat(collection.deriveDocumentId(json("_id: {$in: []}"))).isInstanceOf(ObjectId.class);
    }

    @Test
    public void testSetIdOnUpsert_docDoesNotExist() {
        final Document doc = json("$set: { '_id': 'someid', 'somekey': 'somevalue' }");

        collection.updateDocuments(new Document(), doc, false, true);
    }

    @Test(expected = ImmutableFieldException.class)
    public void testSetIdOnUpdate_docDoesExist() {
        final Document existingDoc = json("'_id': 'arandomid', 'somekey': 'somevalue'");

        this.collection = new TestCollection("some database", "some collection", "_id", Lists.newArrayList(existingDoc));

        final Document selector = json("'somekey':'somevalue'");
        final Document doc = json("$set: { '_id': 'someid', 'somekey': 'somevalue' }");

        collection.updateDocuments(selector, doc, false, true);
    }
}
