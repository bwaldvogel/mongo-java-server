package de.bwaldvogel.mongo.backend.aggregation;

import static de.bwaldvogel.mongo.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.junit.Test;

import de.bwaldvogel.mongo.exception.MongoServerError;

public class AggregationTest {

    @Test
    public void testProject() throws Exception {
        assertThat(Aggregation.projectDocument(json("a: 'value'"), json("a: true"))).isEqualTo(json("a: 'value'"));
        assertThat(Aggregation.projectDocument(json("_id: 1"), json("a: 1"))).isEqualTo(json("_id: 1"));
        assertThat(Aggregation.projectDocument(json("_id: 1, a: 'value'"), json("a: 1"))).isEqualTo(json("_id: 1, a: 'value'"));
    }

    @Test
    public void testIllegalProject() throws Exception {
        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> Aggregation.projectDocument(json(""), json("")))
            .withMessage("Invalid $project :: caused by :: specification must have at least one field");
    }

}