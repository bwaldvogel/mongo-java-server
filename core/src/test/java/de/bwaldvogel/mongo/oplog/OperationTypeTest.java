package de.bwaldvogel.mongo.oplog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.junit.jupiter.api.Test;

class OperationTypeTest {

    @Test
    void testGetCode() {
        assertThat(OperationType.INSERT.getCode()).isEqualTo("i");
        assertThat(OperationType.UPDATE.getCode()).isEqualTo("u");
        assertThat(OperationType.DELETE.getCode()).isEqualTo("d");
    }

    @Test
    void testFromCode() {
        for (OperationType ot : OperationType.values()) {
            OperationType ot1 = OperationType.fromCode(ot.getCode());
            assertThat(ot1).isEqualTo(ot);
        }
    }

    @Test()
    void testFromCode_throwException() {
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> OperationType.fromCode("wrongCode"))
            .withMessage("unknown operation type: wrongCode");
    }
}
