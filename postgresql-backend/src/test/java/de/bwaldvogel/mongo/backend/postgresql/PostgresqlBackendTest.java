package de.bwaldvogel.mongo.backend.postgresql;

import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.AbstractBackendTest;

public class PostgresqlBackendTest extends AbstractBackendTest {

    private static HikariDataSource dataSource;

    @BeforeClass
    public static void initializeDataSource() {
        HikariConfig config = new HikariConfig();

        config.setJdbcUrl("jdbc:postgresql://localhost/mongo-java-server-test");
        config.setUsername("mongo-java-server-test");
        config.setPassword("mongo-java-server-test");
        config.setMaximumPoolSize(5);

        dataSource = new HikariDataSource(config);
    }

    @AfterClass
    public static void closeDataSource() {
        dataSource.close();
    }

    @Override
    protected MongoBackend createBackend() throws Exception {
        PostgresqlBackend backend = new PostgresqlBackend(dataSource);
        for (String db : Arrays.asList(TEST_DATABASE_NAME, OTHER_TEST_DATABASE_NAME)) {
            MongoDatabase mongoDatabase = backend.openOrCreateDatabase(db);
            mongoDatabase.drop();
        }
        return backend;
    }

    @Override
    public void testCompoundSparseUniqueIndex() throws Exception {
        assumeStrictTests();
        super.testCompoundSparseUniqueIndex();
    }

    @Override
    public void testCompoundSparseUniqueIndexOnEmbeddedDocuments() throws Exception {
        assumeStrictTests();
        super.testCompoundSparseUniqueIndexOnEmbeddedDocuments();
    }

    @Override
    public void testSparseUniqueIndexOnEmbeddedDocument() throws Exception {
        assumeStrictTests();
        super.testSparseUniqueIndexOnEmbeddedDocument();
    }

    @Override
    public void testSecondarySparseUniqueIndex() throws Exception {
        assumeStrictTests();
        super.testSecondarySparseUniqueIndex();
    }

    @Override
    public void testUniqueIndexWithDeepDocuments() throws Exception {
        assumeStrictTests();
        super.testUniqueIndexWithDeepDocuments();
    }

    @Override
    public void testOrderByEmbeddedDocument() throws Exception {
        assumeStrictTests();
        super.testOrderByEmbeddedDocument();
    }

    @Override
    public void testOrderByMissingAndNull() throws Exception {
        assumeStrictTests();
        super.testOrderByMissingAndNull();
    }

    @Override
    public void testSortDocuments() throws Exception {
        assumeStrictTests();
        super.testSortDocuments();
    }

    @Override
    public void testFindAndOrderByWithListValues() throws Exception {
        assumeStrictTests();
        super.testFindAndOrderByWithListValues();
    }

    @Override
    public void testSort() {
        assumeStrictTests();
        super.testSort();
    }

    @Override
    public void testInsertQueryAndSortBinaryTypes() throws Exception {
        assumeStrictTests();
        super.testInsertQueryAndSortBinaryTypes();
    }

    @Override
    public void testUuidAsId() throws Exception {
        assumeStrictTests();
        super.testUuidAsId();
    }

    @Override
    public void testTypeMatching() throws Exception {
        assumeStrictTests();
        super.testTypeMatching();
    }

    @Override
    public void testDecimal128() throws Exception {
        assumeStrictTests();
        super.testDecimal128();
    }

    @Override
    public void testArrayNe() throws Exception {
        assumeStrictTests();
        super.testArrayNe();
    }

    private void assumeStrictTests() {
        Assume.assumeTrue(Boolean.getBoolean(PostgresqlBackend.class.getSimpleName() + ".strictTest"));
    }

}