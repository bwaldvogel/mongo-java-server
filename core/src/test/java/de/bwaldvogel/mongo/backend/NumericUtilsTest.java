package de.bwaldvogel.mongo.backend;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.math.BigDecimal;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import de.bwaldvogel.mongo.bson.Decimal128;

public class NumericUtilsTest {

    @Test
    void testAddNumbers() {
        assertThat(NumericUtils.addNumbers(-4, 3)).isEqualTo(-1);
        assertThat(NumericUtils.addNumbers(-0.1, 0.1)).isEqualTo(0.0);
        assertThat(NumericUtils.addNumbers(0.9, 0.1)).isEqualTo(1.0);
        assertThat(NumericUtils.addNumbers(4.3f, 7.1f)).isEqualTo(11.4f);
        assertThat(NumericUtils.addNumbers((short) 4, (short) 7)).isEqualTo((short) 11);
        assertThat(NumericUtils.addNumbers((short) 30000, (short) 20000)).isEqualTo(50000L);
        assertThat(NumericUtils.addNumbers(4L, 7.3)).isEqualTo(11.3);
        assertThat(NumericUtils.addNumbers(100000000000000L, 100000000000000L)).isEqualTo(200000000000000L);
        assertThat(NumericUtils.addNumbers(2000000000, 2000000000)).isEqualTo(4000000000L);
        assertThat(NumericUtils.addNumbers(Decimal128.fromNumber(1L), Decimal128.fromNumber(2.5))).isEqualTo(Decimal128.fromNumber(3.5));

        assertThatExceptionOfType(UnsupportedOperationException.class)
            .isThrownBy(() -> NumericUtils.addNumbers(new BigDecimal(1), new BigDecimal(1)))
            .withMessage("cannot calculate on 1 and 1");
    }

    @Test
    void testSubtractNumbers() {
        assertThat(NumericUtils.subtractNumbers(-4, 3)).isEqualTo(-7);
        assertThat(NumericUtils.subtractNumbers(0.1, 0.1)).isEqualTo(0.0);
        assertThat(NumericUtils.subtractNumbers(1.1, 0.1)).isEqualTo(1.0);
        assertThat(NumericUtils.subtractNumbers(7.6f, 4.1f)).isEqualTo(3.5f);
        assertThat(NumericUtils.subtractNumbers((short) 4, (short) 7)).isEqualTo((short) -3);
        assertThat(NumericUtils.subtractNumbers((short) 30000, (short) -20000)).isEqualTo(50000L);
        assertThat(NumericUtils.subtractNumbers(4L, 7.3)).isEqualTo(-3.3);
        assertThat(NumericUtils.subtractNumbers(100000000000000L, 1L)).isEqualTo(99999999999999L);
        assertThat(NumericUtils.subtractNumbers(-2000000000, 2000000000)).isEqualTo(-4000000000L);
        assertThat(NumericUtils.subtractNumbers(Decimal128.fromNumber(1005L), Decimal128.fromNumber(2.5))).isEqualTo(Decimal128.fromNumber(1002.5));

        assertThatExceptionOfType(UnsupportedOperationException.class)
            .isThrownBy(() -> NumericUtils.subtractNumbers(new BigDecimal(1), new BigDecimal(1)))
            .withMessage("cannot calculate on 1 and 1");
    }

    @Test
    void testMultiplyNumbers() {
        assertThat(NumericUtils.multiplyNumbers(-4, 3)).isEqualTo(-12);
        assertThat((double) NumericUtils.multiplyNumbers(0.1, 0.1)).isEqualTo(0.01, Offset.offset(0.0001));
        assertThat((double) NumericUtils.multiplyNumbers(1.1, 0.1)).isEqualTo(0.11, Offset.offset(0.0001));
        assertThat(NumericUtils.multiplyNumbers(2.0f, 4.0f)).isEqualTo(8.0f);
        assertThat(NumericUtils.multiplyNumbers((short) 4, (short) 7)).isEqualTo((short) 28);
        assertThat(NumericUtils.multiplyNumbers((short) 400, (short) 700)).isEqualTo(280000L);
        assertThat(NumericUtils.multiplyNumbers(100000000000000L, 10)).isEqualTo(1000000000000000L);
        assertThat(NumericUtils.multiplyNumbers(50000, 100000)).isEqualTo(5000000000L);
        assertThat(NumericUtils.multiplyNumbers(Decimal128.fromNumber(1000), Decimal128.fromNumber(2000.5))).isEqualTo(Decimal128.fromNumber(2000500.0));

        assertThatExceptionOfType(UnsupportedOperationException.class)
            .isThrownBy(() -> NumericUtils.multiplyNumbers(new BigDecimal(1), new BigDecimal(1)))
            .withMessage("cannot calculate on 1 and 1");
    }

}
