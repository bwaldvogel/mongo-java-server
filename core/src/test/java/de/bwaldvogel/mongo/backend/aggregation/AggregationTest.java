package de.bwaldvogel.mongo.backend.aggregation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.junit.Test;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class AggregationTest {

    @Test
    public void testProject() throws Exception {
        assertThat(Aggregation.projectDocument(new Document("a", "value"), new Document("a", true))).isEqualTo(new Document("a", "value"));
        assertThat(Aggregation.projectDocument(new Document("_id", 1), new Document("a", 1))).isEqualTo(new Document("_id", 1));
        assertThat(Aggregation.projectDocument(new Document("_id", 1).append("a", "value"), new Document("a", 1))).isEqualTo(new Document("_id", 1).append("a", "value"));
    }

    @Test
    public void testIllegalProject() throws Exception {
        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Aggregation.projectDocument(new Document(), new Document()))
            .withMessage("Invalid $project :: caused by :: specification must have at least one field");
    }

}