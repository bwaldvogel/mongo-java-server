package de.bwaldvogel.mongo.exception;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.junit.Test;

public class MongoServerExceptionTest {

    @Test
    public void testIllegalMessage() throws Exception {
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> new MongoServerException(null))
            .withMessage("Illegal error message");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> new MongoServerException(""))
            .withMessage("Illegal error message");
    }

}