package de.bwaldvogel.mongo.backend;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Date;

import org.bson.BasicBSONObject;
import org.junit.Test;

public class UtilsTest {

    @Test
    public void testAddNumbers() {
        assertThat(Utils.addNumbers(-4, 3)).isEqualTo(Integer.valueOf(-1));
        assertThat(Utils.addNumbers(-0.1, 0.1)).isEqualTo(Double.valueOf(0.0));
        assertThat(Utils.addNumbers(0.9, 0.1)).isEqualTo(Double.valueOf(1.0));
        assertThat(Utils.addNumbers(4.3f, 7.1f)).isEqualTo(Float.valueOf(11.4f));
        assertThat(Utils.addNumbers((short) 4, (short) 7)).isEqualTo(Short.valueOf((short) 11));
        assertThat(Utils.addNumbers(4l, 7.3)).isEqualTo(Double.valueOf(11.3));
        assertThat(Utils.addNumbers(100000000000000l, 100000000000000l)).isEqualTo(Long.valueOf(200000000000000l));
    }

    @Test
    public void testNormalizeValue() {
        assertThat(Utils.normalizeValue(Integer.valueOf(4))).isEqualTo(4.0);
        assertThat(Utils.normalizeValue(null)).isNull();
        assertThat(Utils.normalizeValue(new Date())).isInstanceOf(Date.class);
    }

    @Test
    public void testNullAwareEquals() {
        assertThat(Utils.nullAwareEquals(null, null)).isTrue();
        assertThat(Utils.nullAwareEquals(null, 4)).isFalse();
        assertThat(Utils.nullAwareEquals(4, null)).isFalse();
        assertThat(Utils.nullAwareEquals(4, 4)).isTrue();
        assertThat(Utils.nullAwareEquals(4, 4.0)).isTrue();
        assertThat(Utils.nullAwareEquals(Float.valueOf(3.0f), Double.valueOf(3.0))).isTrue();
    }

    @Test
    public void testCalculateSize() {
        assertThat(Utils.calculateSize(new BasicBSONObject())).isEqualTo(5);
        assertThat(Utils.calculateSize(new BasicBSONObject("_id", 7))).isEqualTo(14);
    }

}
