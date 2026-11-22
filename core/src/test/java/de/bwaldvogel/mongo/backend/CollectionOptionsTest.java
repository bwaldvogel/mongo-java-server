package de.bwaldvogel.mongo.backend;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.junit.jupiter.api.Test;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;

class CollectionOptionsTest {

    @Test
    void testCappedOptionsRoundTrip() {
        Document document = new Document("capped", true).append("size", 513L).append("max", 3L);
        CollectionOptions options = CollectionOptions.fromQuery(document);
        options.validate();
        assertThat(options.toDocument()).isEqualTo(document);
    }

    @Test
    void testCappedSizeMustBePositive() {
        for (long size : new long[] {-1, 0}) {
            CollectionOptions options = CollectionOptions.fromQuery(new Document("capped", true).append("size", size));
            assertThatExceptionOfType(MongoServerError.class).isThrownBy(options::validate)
                .extracting(MongoServerError::getCode).isEqualTo(51024);
        }
    }

    @Test
    void testDefaultOptionsAreNotCapped() {
        assertThat(CollectionOptions.withDefaults().isCapped()).isFalse();
        assertThat(CollectionOptions.withDefaults().toDocument()).isEmpty();
    }
}
