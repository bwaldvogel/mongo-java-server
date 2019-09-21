package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.backend.TestUtils.json;
import static org.assertj.core.api.Assertions.fail;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.function.BooleanSupplier;

import org.bson.BSON;
import org.bson.BasicBSONObject;
import org.bson.Document;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractProtocolTest extends AbstractTest {

    private static final Logger log = LoggerFactory.getLogger(AbstractProtocolTest.class);

    @Test
    public void testInsertOperation() throws Exception {
        assertThat(collection.countDocuments()).isZero();

        try (Socket socket = new Socket(serverAddress.getAddress(), serverAddress.getPort())) {
            OutputStream outputStream = socket.getOutputStream();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            // header
            writeInt(baos, 1); // requestID
            writeInt(baos, 0); // responseTo
            writeInt(baos, 2002); // OP_INSERT

            // content
            writeInt(baos, 0); // flags
            writeString(baos, collection.getNamespace().getFullName());

            writeBson(baos, json("_id: 1"));

            byte[] bytes = baos.toByteArray();
            writeInt(outputStream, bytes.length + 4);
            outputStream.write(bytes);
            outputStream.flush();
        }

        awaitDocumentCount(() -> collection.estimatedDocumentCount() == 1);
        assertThat(collection.find()).containsExactly(json("_id: 1"));
    }

    @Test
    public void testDeleteOperation() throws Exception {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3"));

        assertThat(collection.countDocuments()).isEqualTo(3);

        try (Socket socket = new Socket(serverAddress.getAddress(), serverAddress.getPort())) {
            OutputStream outputStream = socket.getOutputStream();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            // header
            writeInt(baos, 1); // requestID
            writeInt(baos, 0); // responseTo
            writeInt(baos, 2006); // OP_DELETE

            // content
            writeInt(baos, 0); // RESERVED
            writeString(baos, collection.getNamespace().getFullName());
            writeInt(baos, 0); // flags

            writeBson(baos, new Document()); // selector

            byte[] bytes = baos.toByteArray();
            writeInt(outputStream, bytes.length + 4);
            outputStream.write(bytes);
            outputStream.flush();
        }

        awaitDocumentCount(() -> collection.estimatedDocumentCount() == 0);
        assertThat(collection.countDocuments()).isZero();
    }

    @Test
    public void testSingleDeleteOperation() throws Exception {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3"));

        assertThat(collection.countDocuments()).isEqualTo(3);

        try (Socket socket = new Socket(serverAddress.getAddress(), serverAddress.getPort())) {
            OutputStream outputStream = socket.getOutputStream();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            // header
            writeInt(baos, 1); // requestID
            writeInt(baos, 0); // responseTo
            writeInt(baos, 2006); // OP_DELETE

            // content
            writeInt(baos, 0); // RESERVED
            writeString(baos, collection.getNamespace().getFullName());
            writeInt(baos, 1); // SINGLE DOCUMENT

            writeBson(baos, new Document()); // selector

            byte[] bytes = baos.toByteArray();
            writeInt(outputStream, bytes.length + 4);
            outputStream.write(bytes);
            outputStream.flush();
        }

        awaitDocumentCount(() -> collection.estimatedDocumentCount() == 2);
        assertThat(collection.find()).containsExactly(
            json("_id: 2"),
            json("_id: 3")
        );
    }

    @Test
    public void testUpdateOperation() throws Exception {
        collection.insertOne(json("_id: 1"));
        collection.insertOne(json("_id: 2"));
        collection.insertOne(json("_id: 3"));

        assertThat(collection.countDocuments()).isEqualTo(3);

        try (Socket socket = new Socket(serverAddress.getAddress(), serverAddress.getPort())) {
            OutputStream outputStream = socket.getOutputStream();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            // header
            writeInt(baos, 1); // requestID
            writeInt(baos, 0); // responseTo
            writeInt(baos, 2001); // OP_UPDATE

            // content
            writeInt(baos, 0); // RESERVED
            writeString(baos, collection.getNamespace().getFullName());
            writeInt(baos, 1 | 1 << 1); // UPSERT + MULTI_UPDATE

            writeBson(baos, json("_id: {$gte: 2}")); // selector
            writeBson(baos, json("$set: {a: 2}")); // update

            byte[] bytes = baos.toByteArray();
            writeInt(outputStream, bytes.length + 4);
            outputStream.write(bytes);
            outputStream.flush();
        }

        awaitDocumentCount(() -> collection.countDocuments(json("a: 2")) == 2);
        assertThat(collection.find()).containsExactly(
            json("_id: 1"),
            json("_id: 2, a: 2"),
            json("_id: 3, a: 2")
        );
    }

    private void awaitDocumentCount(BooleanSupplier stoppingCriterion) throws Exception {
        for (int i = 0; i < 10; i++) {
            if (stoppingCriterion.getAsBoolean()) {
                log.info("Stopping criterion reached.");
                return;
            } else {
                log.info("Stopping criterion not yet reached. Waiting…");
                Thread.sleep(50);
            }
        }
        fail("Timeout waiting for change");
    }

    private void writeString(OutputStream outputStream, String string) throws Exception {
        outputStream.write(string.getBytes(StandardCharsets.UTF_8));
        writeByte(outputStream, 0);
    }

    private void writeByte(OutputStream outputStream, int value) throws Exception {
        outputStream.write(value);
    }

    private void writeBson(OutputStream outputStream, Document data) throws Exception {
        outputStream.write(BSON.encode(new BasicBSONObject(data)));
    }

    private void writeInt(OutputStream out, int value) throws Exception {
        writeByte(out, value & 0xFF);
        writeByte(out, (value >>> 8) & 0xFF);
        writeByte(out, (value >>> 16) & 0xFF);
        writeByte(out, (value >>> 24) & 0xFF);
    }

}
