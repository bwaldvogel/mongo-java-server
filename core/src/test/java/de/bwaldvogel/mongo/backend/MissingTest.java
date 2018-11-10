package de.bwaldvogel.mongo.backend;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class MissingTest {

    @Test
    public void testIsNullOrMissing() throws Exception {
        assertThat(Missing.isNullOrMissing(null)).isTrue();
        assertThat(Missing.isNullOrMissing(Missing.getInstance())).isTrue();
        assertThat(Missing.isNullOrMissing("value")).isFalse();
    }

}