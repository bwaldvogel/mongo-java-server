package de.bwaldvogel.mongo.backend.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.LegacyUUID;
import de.bwaldvogel.mongo.bson.MaxKey;
import de.bwaldvogel.mongo.bson.MinKey;

class JsonConverterTest {

    @Test
    void testSerializeAndDeserialize_MinKey() throws Exception {
        String json = JsonConverter.toJson(new Document("_id", MinKey.getInstance()));
        Document document = JsonConverter.fromJson(json);
        assertThat(document.get("_id")).isInstanceOf(MinKey.class);
    }

    @Test
    void testSerializeAndDeserialize_MaxKey() throws Exception {
        String json = JsonConverter.toJson(new Document("_id", MaxKey.getInstance()));
        Document document = JsonConverter.fromJson(json);
        assertThat(document.get("_id")).isInstanceOf(MaxKey.class);
    }

    @Test
    void testSerializeLegacyUUID() throws Exception {
        LegacyUUID uuid = new LegacyUUID(1, 2);
        String json = JsonConverter.toJson(new Document("_id", uuid));
        Document mappedDocument = JsonConverter.fromJson(json);
        assertThat(mappedDocument.get("_id")).isEqualTo(uuid);
    }

}
