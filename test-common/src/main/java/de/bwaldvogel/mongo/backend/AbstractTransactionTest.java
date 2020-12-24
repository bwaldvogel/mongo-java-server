package de.bwaldvogel.mongo.backend;

import static com.mongodb.client.model.Updates.set;
import static de.bwaldvogel.mongo.backend.TestUtils.json;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;

public abstract class AbstractTransactionTest extends AbstractTest {

    private static final Logger log = LoggerFactory.getLogger(AbstractTransactionTest.class);

    @Test
    public void testTransactionUpdate() {
        collection.insertOne(json("_id: 5, value: 100"));
        ClientSession clientSession = syncClient.startSession();
        TransactionOptions txnOptions = TransactionOptions.builder()
            .readPreference(ReadPreference.primary())
            .readConcern(ReadConcern.LOCAL)
            .writeConcern(WriteConcern.MAJORITY)
            .build();
        clientSession.startTransaction(txnOptions);
        collection.updateOne(clientSession, json("_id: 5"), set("value", 3));
        collection.updateOne(clientSession, json("_id: 5"), set("value", 2));

        Document doc = collection.find(json("_id: 5")).first();
        assertThat(doc).isNotNull();
        assertThat(doc.get("value")).isEqualTo(100);

        try {
            clientSession.commitTransaction();
        } catch (RuntimeException e) {
            log.error(e.getMessage());
        } finally {
            clientSession.close();
        }

        doc = collection.find(json("_id: 5")).first();
        assertThat(doc).isNotNull();
        assertThat(doc.get("value")).isEqualTo(2);
    }
}