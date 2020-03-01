package de.bwaldvogel.mongo.bson;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.math.BigDecimal;

import org.junit.Test;

import nl.jqno.equalsverifier.EqualsVerifier;

public class Decimal128Test {

    @Test
    public void testEqualsAndHashCodeContract() throws Exception {
        EqualsVerifier.forClass(Decimal128.class).verify();
    }

    @Test
    public void testFromAndToBigDecimal() throws Exception {
        assertThat(new Decimal128(new BigDecimal("1.2345")).toBigDecimal().toString()).isEqualTo("1.2345");
        assertThat(new Decimal128(new BigDecimal("1.2345678901234567890123465789")).toBigDecimal().toString()).isEqualTo("1.2345678901234567890123465789");
        assertThat(new Decimal128(new BigDecimal("12345678901234567890123465789")).toBigDecimal().toString()).isEqualTo("12345678901234567890123465789");
        assertThat(new Decimal128(new BigDecimal("-1.2345")).toBigDecimal().toString()).isEqualTo("-1.2345");
        assertThat(new Decimal128(new BigDecimal("0.0")).toBigDecimal().toString()).isEqualTo("0.0");
        assertThat(new Decimal128(new BigDecimal("0").scaleByPowerOfTen(100)).toBigDecimal().toString()).isEqualTo("0E+100");
        assertThat(new Decimal128(new BigDecimal("-0")).toBigDecimal().toString()).isEqualTo("0");
    }

    @Test
    public void testFromBigDecimal_NumberFormatException() {
        assertThatExceptionOfType(NumberFormatException.class)
            .isThrownBy(() -> new Decimal128(new BigDecimal("2").scaleByPowerOfTen(10000)))
            .withMessage("Exponent is out of range for Decimal128 encoding of 2E+10000");

        assertThatExceptionOfType(NumberFormatException.class)
            .isThrownBy(() -> new Decimal128(new BigDecimal("1.23456789012345678901234657890123456789")))
            .withMessage("Conversion to Decimal128 would require inexact rounding of 1.23456789012345678901234657890123456789");

        assertThatExceptionOfType(NumberFormatException.class)
            .isThrownBy(() -> new Decimal128(new BigDecimal("2").scaleByPowerOfTen(-10000)))
            .withMessage("Conversion to Decimal128 would require inexact rounding of 2E-10000");
    }

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

    @Test
    public void testToString() throws Exception {
        assertThat(new Decimal128(new BigDecimal("1e1000"))).hasToString("1E+1000");
        assertThat(new Decimal128(new BigDecimal("1000"))).hasToString("1000");
        assertThat(new Decimal128(new BigDecimal("1000.987654321"))).hasToString("1000.987654321");
        assertThat(new Decimal128(new BigDecimal("1000").scaleByPowerOfTen(2))).hasToString("1.000E+5");
        assertThat(Decimal128.POSITIVE_INFINITY).hasToString("Infinity");
        assertThat(Decimal128.NEGATIVE_INFINITY).hasToString("-Infinity");
        assertThat(Decimal128.NaN).hasToString("NaN");
    }

}
