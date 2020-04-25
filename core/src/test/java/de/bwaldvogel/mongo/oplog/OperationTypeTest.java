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
        for (OperationType operationType : OperationType.values()) {
            OperationType operationTypeFromCode = OperationType.fromCode(operationType.getCode());
            assertThat(operationTypeFromCode).isEqualTo(operationType);
        }
    }

    @Test()
    void testFromCode_throwException() {
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> OperationType.fromCode("wrongCode"))
            .withMessage("unknown operation type: wrongCode");
    }
}
