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
    public void testSimpleTransaction() {
        collection.insertOne(json("_id: 100, value: 1"));
        ClientSession clientSession = syncClient.startSession();
        TransactionOptions txnOptions = TransactionOptions.builder()
            .readPreference(ReadPreference.primary())
            .readConcern(ReadConcern.LOCAL)
            .writeConcern(WriteConcern.MAJORITY)
            .build();
        clientSession.startTransaction(txnOptions);
        collection.insertOne(clientSession, json("_id: 1, name: \"testDoc\""));

        try {
            clientSession.commitTransaction();
        } catch (RuntimeException e) {
            log.error(e.getMessage());
            // some error handling
        } finally {
            clientSession.close();
        }
        Document doc = collection.find(json("_id: 1")).first();
        assertThat(doc).isNotNull();
        assertThat(doc.get("name")).isEqualTo("testDoc");
    }

    @Test
    public void testTransactionShouldOnlyApplyChangesAfterCommitting() throws InterruptedException {
        collection.insertOne(json("_id: 1, value: 1"));
        ClientSession clientSession = syncClient.startSession();
        clientSession.startTransaction();
        collection.updateOne(clientSession, json("_id: 1"), set("value", 2));
        collection.updateOne(clientSession, json("_id: 1"), set("value", 4));

        Document doc = collection.find(json("_id: 1")).first();
        assertThat(doc).isNotNull();
        assertThat(doc.get("value")).isEqualTo(1);

        try {
            clientSession.commitTransaction();
        } catch (RuntimeException e) {
            System.out.println(e.getMessage());

        } finally {
            clientSession.close();
        }

        doc = collection.find(json("_id: 1")).first();
        assertThat(doc).isNotNull();
        assertThat(doc.get("value")).isEqualTo(4);
        Thread.yield();
    }

}