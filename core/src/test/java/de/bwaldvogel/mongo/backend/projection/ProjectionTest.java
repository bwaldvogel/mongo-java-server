package de.bwaldvogel.mongo.backend.projection;

import static de.bwaldvogel.mongo.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class ProjectionTest {

    @Test
    public void testProjectDocument() throws Exception {
        assertThat(Projection.projectDocument(null, null, null)).isNull();

        assertThat(Projection.projectDocument(json("_id: 100"), json("_id: 1"), "_id"))
            .isEqualTo(json("_id: 100"));

        assertThat(Projection.projectDocument(json("_id: 100, foo: 123"), json("foo: 1"), "_id"))
            .isEqualTo(json("_id: 100, foo: 123"));

        assertThat(Projection.projectDocument(json("_id: 100, foo: 123"), json("_id: 1"), "_id"))
            .isEqualTo(json("_id: 100"));

        assertThat(Projection.projectDocument(json("_id: 100, foo: 123"), json("_id: 0, foo: 1"), "_id"))
            .isEqualTo(json("foo: 123"));

        assertThat(Projection.projectDocument(json("_id: 100, foo: 123, bar: 456"), json("_id: 0, foo: 1"), "_id"))
            .isEqualTo(json("foo: 123"));

        assertThat(Projection.projectDocument(json("_id: 100, foo: 123, bar: 456"), json("foo: 0"), "_id"))
            .isEqualTo(json("_id: 100, bar: 456"));

        assertThat(Projection.projectDocument(json("_id: 1, foo: {bar: 123, bla: 'x'}"), json("'foo.bar': 1"), "_id"))
            .isEqualTo(json("_id: 1, foo: {bar: 123}"));

        assertThat(Projection.projectDocument(json("_id: 1"), json("'foo.bar': 1"), "_id"))
            .isEqualTo(json("_id: 1"));

        assertThat(Projection.projectDocument(json("_id: 1, foo: {a: 'x', b: 'y', c: 'z'}"), json("'foo.a': 1, 'foo.c': 1"), "_id"))
            .isEqualTo(json("_id: 1, foo: {a: 'x', c: 'z'}"));
    }

}