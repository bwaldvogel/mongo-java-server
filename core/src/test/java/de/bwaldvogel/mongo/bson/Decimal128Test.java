package de.bwaldvogel.mongo.bson;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.junit.Test;

public class Decimal128Test {

    @Test
    public void testToInt() throws Exception {
        assertThat(Decimal128.ONE.intValue()).isEqualTo(1);
        assertThat(Decimal128.POSITIVE_ZERO.intValue()).isEqualTo(0);
        assertThat(Decimal128.NEGATIVE_ZERO.intValue()).isEqualTo(0);

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(Decimal128.NaN::intValue)
            .withMessage("NaN cannot be converted to BigDecimal");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(Decimal128.POSITIVE_INFINITY::intValue)
            .withMessage("Infinity cannot be converted to BigDecimal");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(Decimal128.NEGATIVE_INFINITY::intValue)
            .withMessage("-Infinity cannot be converted to BigDecimal");
    }

    @Test
    public void testToLong() throws Exception {
        assertThat(Decimal128.ONE.longValue()).isEqualTo(1L);
        assertThat(Decimal128.POSITIVE_ZERO.longValue()).isEqualTo(0L);
        assertThat(Decimal128.NEGATIVE_ZERO.longValue()).isEqualTo(0L);

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(Decimal128.NaN::longValue)
            .withMessage("NaN cannot be converted to BigDecimal");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(Decimal128.POSITIVE_INFINITY::longValue)
            .withMessage("Infinity cannot be converted to BigDecimal");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(Decimal128.NEGATIVE_INFINITY::longValue)
            .withMessage("-Infinity cannot be converted to BigDecimal");
    }

    @Test
    public void testToFloat() throws Exception {
        assertThat(Decimal128.ONE.floatValue()).isEqualTo(1.0f);
        assertThat(Decimal128.POSITIVE_ZERO.floatValue()).isEqualTo(0.0f);
        assertThat(Decimal128.NEGATIVE_ZERO.floatValue()).isEqualTo(0.0f);
        assertThat(Decimal128.NaN.floatValue()).isNaN();
        assertThat(Decimal128.POSITIVE_INFINITY.floatValue()).isEqualTo(Float.POSITIVE_INFINITY);
        assertThat(Decimal128.NEGATIVE_INFINITY.floatValue()).isEqualTo(Float.NEGATIVE_INFINITY);
    }

    @Test
    public void testToDouble() throws Exception {
        assertThat(Decimal128.ONE.doubleValue()).isEqualTo(1.0);
        assertThat(Decimal128.POSITIVE_ZERO.doubleValue()).isEqualTo(0.0);
        assertThat(Decimal128.NEGATIVE_ZERO.doubleValue()).isEqualTo(0.0);
        assertThat(Decimal128.NaN.doubleValue()).isNaN();
        assertThat(Decimal128.POSITIVE_INFINITY.doubleValue()).isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(Decimal128.NEGATIVE_INFINITY.doubleValue()).isEqualTo(Double.NEGATIVE_INFINITY);
    }

}
