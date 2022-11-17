package de.bwaldvogel.mongo.backend.postgresql;

import static de.bwaldvogel.mongo.backend.TestUtils.json;
import static de.bwaldvogel.mongo.backend.TestUtils.toArray;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import com.mongodb.DuplicateKeyException;
import com.mongodb.client.model.IndexOptions;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.AbstractBackendTest;
import de.bwaldvogel.mongo.oplog.NoopOplog;

class PostgresqlBackendTest extends AbstractBackendTest {

    private static GenericContainer<?> postgresContainer;
    private static HikariDataSource dataSource;

    @BeforeAll
    static void initializeDataSource() {
        String password = UUID.randomUUID().toString();
        postgresContainer = new GenericContainer<>("postgres:9.6-alpine")
            .withTmpFs(Collections.singletonMap("/var/lib/postgresql/data", "rw"))
            .withEnv("POSTGRES_USER", "mongo-java-server-test")
            .withEnv("POSTGRES_PASSWORD", password)
            .withEnv("POSTGRES_DB", "mongo-java-server-test")
            .withExposedPorts(5432);
        postgresContainer.start();

        HikariConfig config = new HikariConfig();

        config.setJdbcUrl("jdbc:postgresql://localhost:" + postgresContainer.getFirstMappedPort() + "/mongo-java-server-test");
        config.setUsername("mongo-java-server-test");
        config.setPassword(password);
        config.setMaximumPoolSize(5);

        dataSource = new HikariDataSource(config);
    }

    @AfterAll
    static void closeDataSource() {
        dataSource.close();
        postgresContainer.stop();
    }

    @Override
    protected MongoBackend createBackend() throws Exception {
        PostgresqlBackend backend = new PostgresqlBackend(dataSource, clock);
        for (String db : Arrays.asList(TEST_DATABASE_NAME, OTHER_TEST_DATABASE_NAME)) {
            MongoDatabase mongoDatabase = backend.openOrCreateDatabase(db);
            mongoDatabase.drop(NoopOplog.get());
        }
        return backend;
    }

    @Test
    @Override
    public void testCompoundSparseUniqueIndex() throws Exception {
        assumeStrictTests();
        super.testCompoundSparseUniqueIndex();
    }

    @Test
    @Override
    public void testCompoundSparseUniqueIndexOnEmbeddedDocuments() throws Exception {
        assumeStrictTests();
        super.testCompoundSparseUniqueIndexOnEmbeddedDocuments();
    }

    @Test
    @Override
    public void testSparseUniqueIndexOnEmbeddedDocument() throws Exception {
        assumeStrictTests();
        super.testSparseUniqueIndexOnEmbeddedDocument();
    }

    @Test
    @Override
    public void testSecondarySparseUniqueIndex() throws Exception {
        assumeStrictTests();
        super.testSecondarySparseUniqueIndex();
    }

    @Test
    @Override
    public void testUniqueIndexWithDeepDocuments() throws Exception {
        assumeStrictTests();
        super.testUniqueIndexWithDeepDocuments();
    }

    @Test
    @Override
    public void testOrderByEmbeddedDocument() throws Exception {
        assumeStrictTests();
        super.testOrderByEmbeddedDocument();
    }

    @Test
    @Override
    public void testOrderByMissingAndNull() throws Exception {
        assumeStrictTests();
        super.testOrderByMissingAndNull();
    }

    @Test
    @Override
    public void testSortDocuments() throws Exception {
        assumeStrictTests();
        super.testSortDocuments();
    }

    @Test
    @Override
    public void testFindAndOrderByWithListValues() throws Exception {
        assumeStrictTests();
        super.testFindAndOrderByWithListValues();
    }

    @Test
    @Override
    public void testSort() {
        assumeStrictTests();
        super.testSort();
    }

    @Test
    @Override
    public void testInsertQueryAndSortBinaryTypes() throws Exception {
        assumeStrictTests();
        super.testInsertQueryAndSortBinaryTypes();
    }

    @Test
    @Override
    public void testUuidAsId() throws Exception {
        assumeStrictTests();
        super.testUuidAsId();
    }

    @Test
    @Override
    public void testTypeMatching() throws Exception {
        assumeStrictTests();
        super.testTypeMatching();
    }

    @Test
    @Override
    public void testDecimal128() throws Exception {
        assumeStrictTests();
        super.testDecimal128();
    }

    @Test
    @Override
    public void testDecimal128_Inc() throws Exception {
        assumeStrictTests();
        super.testDecimal128_Inc();
    }

    @Test
    @Override
    public void testArrayNe() throws Exception {
        assumeStrictTests();
        super.testArrayNe();
    }

    @Test
    @Override
    public void testRegExQuery() throws Exception {
        assumeStrictTests();
        super.testRegExQuery();
    }

    @Test
    @Override
    public void testInsertAndFindJavaScriptContent() throws Exception {
        assumeStrictTests();
        super.testInsertAndFindJavaScriptContent();
    }

    @Test
    @Override
    public void testMultikeyIndex_simpleArrayValues() throws Exception {
        assumeStrictTests();
        super.testMultikeyIndex_simpleArrayValues();
    }

    @Test
    @Override
    public void testCompoundMultikeyIndex_simpleArrayValues() throws Exception {
        assumeStrictTests();
        super.testCompoundMultikeyIndex_simpleArrayValues();
    }

    @Test
    @Override
    public void testCompoundMultikeyIndex_documents() throws Exception {
        assumeStrictTests();
        super.testCompoundMultikeyIndex_documents();
    }

    @Test
    @Override
    public void testCompoundMultikeyIndex_multiple_document_keys() throws Exception {
        assumeStrictTests();
        super.testCompoundMultikeyIndex_multiple_document_keys();
    }

    @Test
    @Override
    public void testCompoundMultikeyIndex_deepDocuments() throws Exception {
        assumeStrictTests();
        super.testCompoundMultikeyIndex_deepDocuments();
    }

    @Test
    @Override
    public void testCompoundMultikeyIndex_threeKeys() throws Exception {
        assumeStrictTests();
        super.testCompoundMultikeyIndex_threeKeys();
    }

    @Test
    @Override
    public void testEmbeddedSort_arrayOfDocuments() {
        assumeStrictTests();
        super.testEmbeddedSort_arrayOfDocuments();
    }

    @Test
    @Override
    public void testAddUniqueIndexOnExistingDocuments_violatingUniqueness() throws Exception {
        collection.insertOne(json("_id: 1, value: 'a'"));
        collection.insertOne(json("_id: 2, value: 'b'"));
        collection.insertOne(json("_id: 3, value: 'c'"));
        collection.insertOne(json("_id: 4, value: 'b'"));

        assertThatExceptionOfType(DuplicateKeyException.class)
            .isThrownBy(() -> collection.createIndex(json("value: 1"), new IndexOptions().unique(true)))
            .withMessageMatching("Write failed with error code 11000 and error message " +
                "'Index build failed: [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}: " +
                "Collection testdb\\.testcoll \\( [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} \\) :: caused by :: " +
                "E11000 duplicate key error collection: testdb\\.testcoll index: value_1 dup key: " +
                "ERROR: could not create unique index \"testcoll_value_1\"\n  Detail: Key \\(\\(data ->> 'value'::text\\)\\)=\\(b\\) is duplicated\\.'");

        assertThat(collection.listIndexes())
            .containsExactly(json("name: '_id_', key: {_id: 1}, v: 2"));

        collection.insertOne(json("_id: 5, value: 'a'"));
    }

    @Test
    @Override
    public void testUniqueIndexOnArrayField() throws Exception {
        if (isStrictTestsEnabled()) {
            super.testUniqueIndexOnArrayField();
        } else {
            collection.createIndex(json("a: 1"), new IndexOptions().unique(true));

            collection.insertOne(json("_id: 1, a: ['val1', 'val2']"));
            collection.insertOne(json("_id: 2, a: ['val3', 'val4']"));
            collection.insertOne(json("_id: 3, a: []"));
            collection.insertOne(json("_id: 4, a: ['val5']"));

            // this should actually not be allowed
            collection.insertOne(json("_id: 5, a: ['val1']"));

            assertThat(toArray(collection.find(json("a: ['val1', 'val3']"))))
                .isEmpty();

            assertThat(toArray(collection.find(json("a: ['val1', 'val10']"))))
                .isEmpty();

            assertThat(toArray(collection.find(json("a: ['val10']"))))
                .isEmpty();

            assertThat(toArray(collection.find(json("a: ['val5']"))))
                .containsExactly(json("_id: 4, a: ['val5']"));

            assertThat(toArray(collection.find(json("a: ['val1', 'val2']"))))
                .containsExactly(json("_id: 1, a: ['val1', 'val2']"));

            assertThat(toArray(collection.find(json("a: ['val2', 'val1']"))))
                .isEmpty();

            assertThat(toArray(collection.find(json("a: {$all: ['val1', 'val2']}"))))
                .containsExactly(json("_id: 1, a: ['val1', 'val2']"));

            assertThat(toArray(collection.find(json("a: {$all: ['val2', 'val1']}"))))
                .containsExactly(json("_id: 1, a: ['val1', 'val2']"));
        }
    }

    @Test
    @Override
    public void testUniqueIndexOnArrayField_updates() throws Exception {
        if (isStrictTestsEnabled()) {
            super.testUniqueIndexOnArrayField_updates();
        } else {
            collection.createIndex(json("a: 1"), new IndexOptions().unique(true));

            collection.insertOne(json("_id: 1, a: ['val1', 'val2']"));
            collection.insertOne(json("_id: 2, a: ['val3', 'val4']"));

            // this should actually not be allowed
            collection.replaceOne(json("_id: 1"), json("_id: 1, a: ['val1', 'val3']"));
            // undo the change
            collection.replaceOne(json("_id: 1"), json("_id: 1, a: ['val1', 'val2']"));

            // this should actually not be allowed
            collection.updateOne(json("_id: 1"), json("$push: {a: 'val3'}"));
            // undo the change
            collection.replaceOne(json("_id: 1"), json("_id: 1, a: ['val1', 'val2']"));

            collection.replaceOne(json("_id: 1"), json("_id: 1, a: ['val1', 'val5']"));
            collection.insertOne(json("_id: 3, a: ['val2']"));

            assertThat(toArray(collection.find(json("a: ['val1', 'val5']"))))
                .containsExactly(json("_id: 1, a: ['val1', 'val5']"));

            assertThat(toArray(collection.find(json("a: ['val2']"))))
                .containsExactly(json("_id: 3, a: ['val2']"));

            collection.updateOne(json("a: ['val2']"), json("$push: {a: 'val7'}"));

            assertThat(toArray(collection.find(json("a: ['val2', 'val7']"))))
                .containsExactly(json("_id: 3, a: ['val2', 'val7']"));
        }
    }

    @Test
    @Override
    public void testUniqueIndexOnArrayFieldInSubdocument() throws Exception {
        if (isStrictTestsEnabled()) {
            super.testUniqueIndexOnArrayFieldInSubdocument();
        } else {
            collection.createIndex(json("'a.b': 1"), new IndexOptions().unique(true));

            collection.insertOne(json("_id: 1, a: {b: ['val1', 'val2']}"));
            collection.insertOne(json("_id: 2, a: {b: ['val3', 'val4']}"));
            collection.insertOne(json("_id: 3, a: []"));
            collection.insertOne(json("_id: 4, a: {b: 'val5'}"));

            // this should actually not be allowed
            collection.insertOne(json("a: {b: ['val1']}"));

            assertThat(toArray(collection.find(json("'a.b': 'val5'"))))
                .containsExactly(json("_id: 4, a: {b: 'val5'}"));

            assertThat(toArray(collection.find(json("'a.b': ['val1', 'val2']"))))
                .containsExactly(json("_id: 1, a: {b: ['val1', 'val2']}"));

            assertThat(toArray(collection.find(json("a: {b: ['val1', 'val2']}"))))
                .containsExactly(json("_id: 1, a: {b: ['val1', 'val2']}"));
        }
    }

    @Test
    @Override
    public void testGetKeyValues_multiKey_document_nested_objects() throws Exception {
        assumeStrictTests();
        super.testGetKeyValues_multiKey_document_nested_objects();
    }

    @Test
    @Override
    public void testOldAndNewUuidTypes() throws Exception {
        assumeStrictTests();
    }

    @Test
    void testUuidDuplicate() throws Exception {
        Document document1 = new Document("_id", UUID.fromString("5542cbb9-7833-96a2-b456-f13b6ae1bc80"));

        collection.insertOne(document1);

        assertMongoWriteException(() -> collection.insertOne(document1),
            11000, "DuplicateKey", "E11000 duplicate key error collection: testdb.testcoll index:" +
                " ERROR: duplicate key value violates unique constraint \"testcoll__id_\"\n" +
                "  Detail: Key ((data ->> '_id'::text))=([\"java.util.UUID\",\"5542cbb9-7833-96a2-b456-f13b6ae1bc80\"]) already exists.");

        Document document2 = new Document("_id", UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"));
        collection.insertOne(document2);

        assertThat(collection.find())
            .containsExactlyInAnyOrder(
                document1,
                document2
            );
    }

    @Test
    @Override
    public void testUpdatePushSlice() throws Exception {
        assumeStrictTests();
        super.testUpdatePushSlice();
    }

    @Test
    @Override
    public void testUpdatePushSortAndSlice() throws Exception {
        assumeStrictTests();
        super.testUpdatePushSortAndSlice();
    }

    @Test
    @Override
    public void testMinMaxKeyRangeQuery() throws Exception {
        assumeStrictTests();
        super.testMinMaxKeyRangeQuery();
    }

    private void assumeStrictTests() {
        Assumptions.assumeTrue(isStrictTestsEnabled());
    }

    private static boolean isStrictTestsEnabled() {
        return Boolean.getBoolean(PostgresqlBackend.class.getSimpleName() + ".strictTest");
    }

}
