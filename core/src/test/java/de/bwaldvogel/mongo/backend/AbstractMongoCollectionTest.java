package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.stream.Stream;

import de.bwaldvogel.mongo.exception.InvalidOptionsException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.ObjectId;
import de.bwaldvogel.mongo.exception.ConflictingUpdateOperatorsException;

public class AbstractMongoCollectionTest {

    private static class TestCollection extends AbstractMongoCollection<Object> {

        TestCollection(MongoDatabase database, String collectionName) {
            super(database, collectionName, CollectionOptions.withDefaults(), new CursorRegistry());
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
        protected QueryResult matchDocuments(Document query, Document orderBy, int numberToSkip,
                                             int numberToReturn, int batchSize, Document fieldSelector,
                                             MongoSession mongoSession) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void updateDataSize(int sizeDelta) {
        }

        @Override
        protected int getDataSize() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void handleUpdate(Object position, Document oldDocument, Document newDocument) {
            // noop
        }

        @Override
        protected void handleUpdate(Object position, Document oldDocument, Document newDocument, MongoSession mongoSession) {
            throw new InvalidOptionsException("Sessions and transaction is not supported on on this backend, please use H2Backend");
        }

        @Override
        protected Stream<DocumentWithPosition<Object>> streamAllDocumentsWithPosition() {
            throw new UnsupportedOperationException();
        }

    }

    private TestCollection collection;

    @BeforeEach
    void setUp() {
        MongoDatabase database = Mockito.mock(MongoDatabase.class);
        this.collection = new TestCollection(database, "some collection");
    }

    @Test
    void testConvertSelector() throws Exception {
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
    void testDeriveDocumentId() throws Exception {
        assertThat(collection.deriveDocumentId(json(""))).isInstanceOf(ObjectId.class);
        assertThat(collection.deriveDocumentId(json("a: 1"))).isInstanceOf(ObjectId.class);
        assertThat(collection.deriveDocumentId(json("_id: 1"))).isEqualTo(1);
        assertThat(collection.deriveDocumentId(json("_id: {$in: [1]}"))).isEqualTo(1);
        assertThat(collection.deriveDocumentId(json("_id: {$in: []}"))).isInstanceOf(ObjectId.class);
    }

    @Test
    void testValidateUpdateQuery() throws Exception {
        AbstractMongoCollection.validateUpdateQuery(new Document());
        AbstractMongoCollection.validateUpdateQuery(json("$set: {a: 1, b: 1}"));
        AbstractMongoCollection.validateUpdateQuery(json("$set: {a: 1, b: 1}, $inc: {c: 1}"));
        AbstractMongoCollection.validateUpdateQuery(json("$set: {'a.b.c': 1}, $inc: {'a.b.d': 1}"));
        AbstractMongoCollection.validateUpdateQuery(json("$set: {'a.b.c': 1}, $inc: {'a.c': 1}"));
        AbstractMongoCollection.validateUpdateQuery(json("$inc: {'a.$.y': 1, 'a.$.x': 1}"));
        AbstractMongoCollection.validateUpdateQuery(json("$inc: {'a.$.deleted': 1, 'a.$.deletedBy': 1}"));

        assertThatExceptionOfType(ConflictingUpdateOperatorsException.class)
            .isThrownBy(() -> AbstractMongoCollection.validateUpdateQuery(json("$set: {a: 1, b: 1}, $inc: {b: 1}")))
            .withMessage("[Error 40] Updating the path 'b' would create a conflict at 'b'");

        assertThatExceptionOfType(ConflictingUpdateOperatorsException.class)
            .isThrownBy(() -> AbstractMongoCollection.validateUpdateQuery(json("$set: {'a.b.c': 1}, $inc: {'a.b': 1}")))
            .withMessage("[Error 40] Updating the path 'a.b' would create a conflict at 'a.b'");
    }

}
