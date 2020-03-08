package de.bwaldvogel.mongo.exception;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.junit.jupiter.api.Test;

public class MongoServerExceptionTest {

    @Test
    void testIllegalMessage() throws Exception {
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> new MongoServerException(null))
            .withMessage("Illegal error message");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> new MongoServerException(""))
            .withMessage("Illegal error message");
    }

}
