package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.TestUtils.json;
import static de.bwaldvogel.mongo.backend.AbstractMongoBackend.ADMIN_DB_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.ServerVersion;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.InvalidOptionsException;
import de.bwaldvogel.mongo.exception.TypeMismatchException;

class AbstractMongoBackendTest {

    private MongoBackend backend;
    private MongoBackend backendWithError;

    @BeforeEach
    public void setup() {
        backend = new AbstractMongoBackend() {

            @Override
            protected MongoDatabase openOrCreateDatabase(String databaseName) {
                MongoDatabase mockDatabase = Mockito.mock(AbstractMongoDatabase.class);

                Document fakeResponse = new Document();
                Utils.markOkay(fakeResponse);
                fakeResponse.put("message", "fakeResponse");

                when(mockDatabase.handleCommand(any(), any(), any(), any())).thenReturn(fakeResponse);

                return mockDatabase;
            }
        };

        backendWithError = new AbstractMongoBackend() {

            @Override
            protected MongoDatabase openOrCreateDatabase(String databaseName) {
                return null;
            }

            @Override
            public void dropDatabase(String database) {
                throw new RuntimeException("unexpected");
            }
        };
    }

    @Test
    void testHandleAdminCommand() {
        Document response = backend.handleCommand(ADMIN_DB_NAME, DatabaseCommand.of(Command.PING), null);
        assertThat(response).isNotNull();
        assertThat(response.get("ok")).isEqualTo(1.0);
    }

    @Test
    void testMongoDatabaseHandleCommand() {
        Document response = backend.handleCommand("mockDatabase", DatabaseCommand.of(Command.FIND), null);
        assertThat(response).isNotNull();
        assertThat(response.get("ok")).isEqualTo(1.0);
        assertThat(response.get("message")).isEqualTo("fakeResponse");
    }

    @Test
    void testGetParameter() {
        Document response = getParameter(json("getParameter: 1, featureCompatibilityVersion: 1"));
        assertThat(response).isEqualTo(json("featureCompatibilityVersion: {version: '7.0'}, ok: 1.0"));
    }

    @Test
    void testGetParameter_multipleParameters() {
        Document response = getParameter(json("getParameter: 1, quiet: 1, logLevel: 1"));
        assertThat(response).isEqualTo(json("quiet: false, logLevel: 0, ok: 1.0"));
    }

    @Test
    void testGetParameter_ignoresUnknownParameters() {
        Document response = getParameter(json("getParameter: 1, notablescan: 1, unknownParameter: 1"));
        assertThat(response).isEqualTo(json("notablescan: false, ok: 1.0"));
    }

    @Test
    void testGetParameter_featureCompatibilityVersionFollowsServerVersion() {
        backend.version(ServerVersion.MONGO_8_0);
        Document response = getParameter(json("getParameter: 1, featureCompatibilityVersion: 1"));
        assertThat(response).isEqualTo(json("featureCompatibilityVersion: {version: '8.0'}, ok: 1.0"));
    }

    @Test
    void testGetParameter_showDetails() {
        Document response = getParameter(json("getParameter: {showDetails: true}, enableLocalhostAuthBypass: 1"));
        assertThat(response).isEqualTo(json("enableLocalhostAuthBypass:"
            + " {value: true, settableAtRuntime: false, settableAtStartup: true}, ok: 1.0"));
    }

    @Test
    void testGetParameter_allParametersWithStar() {
        Document response = getParameter(json("getParameter: '*'"));
        assertThat(response)
            .containsOnlyKeys(
                "featureCompatibilityVersion",
                "quiet",
                "logLevel",
                "notablescan",
                "maxIndexBuildMemoryUsageMegabytes",
                "enableLocalhostAuthBypass",
                "authenticationMechanisms",
                "clusterAuthMode",
                "ok")
            .containsEntry("maxIndexBuildMemoryUsageMegabytes", 200)
            .containsEntry("authenticationMechanisms", List.of("MONGODB-X509", "SCRAM-SHA-1", "SCRAM-SHA-256"))
            .containsEntry("clusterAuthMode", "undefined");
    }

    @Test
    void testGetParameter_allParametersWithDetails() {
        Document response = getParameter(json("getParameter: {allParameters: true, showDetails: true}"));
        assertThat(response)
            .hasSize(9)
            .containsEntry("quiet", json("value: false, settableAtRuntime: true, settableAtStartup: true"))
            .containsEntry("clusterAuthMode", json("value: 'undefined', settableAtRuntime: true, settableAtStartup: false"));
    }

    @Test
    void testGetParameter_allParametersFalse() {
        Document response = getParameter(json("getParameter: {allParameters: false}, logLevel: 1"));
        assertThat(response).isEqualTo(json("logLevel: 0, ok: 1.0"));
    }

    @Test
    void testGetParameter_noKnownParameter() {
        assertThatExceptionOfType(InvalidOptionsException.class)
            .isThrownBy(() -> getParameter(json("getParameter: 1, unknownParameter: 1")))
            .withMessage("[Error 72] no option found to get");

        assertThatExceptionOfType(InvalidOptionsException.class)
            .isThrownBy(() -> getParameter(json("getParameter: 1")))
            .withMessage("[Error 72] no option found to get");
    }

    @Test
    void testGetParameter_numericOptions() {
        assertThat(getParameter(json("getParameter: {showDetails: 1}, logLevel: 1")))
            .isEqualTo(json("logLevel: {value: 0, settableAtRuntime: true, settableAtStartup: true}, ok: 1.0"));
        assertThat(getParameter(json("getParameter: {showDetails: 0}, logLevel: 1")))
            .isEqualTo(json("logLevel: 0, ok: 1.0"));
        assertThat(getParameter(json("getParameter: {allParameters: 1.0}"))).hasSize(9);
        assertThat(getParameter(json("getParameter: {allParameters: null}, logLevel: 1")))
            .isEqualTo(json("logLevel: 0, ok: 1.0"));
    }

    @Test
    void testGetParameter_nonBooleanOptions() {
        assertThatExceptionOfType(TypeMismatchException.class)
            .isThrownBy(() -> getParameter(json("getParameter: {allParameters: 'true'}")))
            .withMessage("[Error 14] BSON field 'getParameter.allParameters' is the wrong type 'string',"
                + " expected types '[bool, long, int, decimal, double']");

        assertThatExceptionOfType(TypeMismatchException.class)
            .isThrownBy(() -> getParameter(json("getParameter: {showDetails: 'yes'}, logLevel: 1")))
            .withMessage("[Error 14] BSON field 'getParameter.showDetails' is the wrong type 'string',"
                + " expected types '[bool, long, int, decimal, double']");
    }

    private Document getParameter(Document query) {
        return backend.handleCommand(ADMIN_DB_NAME, DatabaseCommand.of(Command.GET_PARAMETER), query);
    }

}
