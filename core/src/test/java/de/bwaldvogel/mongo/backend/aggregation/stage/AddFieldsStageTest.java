package de.bwaldvogel.mongo.backend.aggregation.stage;

import static de.bwaldvogel.mongo.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.junit.Test;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class AddFieldsStageTest {

    @Test
    public void testAddFields() throws Exception {
        assertThat(addFields(json("a: 'value'"), json("a: true"))).isEqualTo(json("a: true"));
        assertThat(addFields(json("_id: 1"), json("a: 10"))).isEqualTo(json("_id: 1, a: 10"));
        assertThat(addFields(json("_id: 1, a: 'value'"), json("b: '$a'"))).isEqualTo(json("_id: 1, a: 'value', b: 'value'"));
        assertThat(addFields(json("_id: 1, a: 'value'"), json("_id: null"))).isEqualTo(json("_id: null, a: 'value'"));
    }

    private static Document addFields(Document document, Document addFields) {
        return new AddFieldsStage(addFields).projectDocument(document);
    }

    @Test
    public void testIllegalSpecification() throws Exception {
        assertThatExceptionOfType(MongoServerError.class)
            .isThrownBy(() -> new AddFieldsStage(json("")))
            .withMessage("[Error 40177] Invalid $addFields :: caused by :: specification must have at least one field");
    }

}