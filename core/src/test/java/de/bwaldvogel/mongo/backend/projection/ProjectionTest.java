package de.bwaldvogel.mongo.backend.projection;

import static de.bwaldvogel.mongo.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import de.bwaldvogel.mongo.bson.Document;

public class ProjectionTest {

    @Test
    public void testProjectDocument() throws Exception {
        assertThat(Projection.projectDocument(null, new Document(), null)).isNull();

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

    @Test
    public void testProjectMissingValue() throws Exception {
        assertThat(Projection.projectDocument(json("_id: 1"), json("'a.b': 1"), "_id"))
            .isEqualTo(json("_id: 1"));

        assertThat(Projection.projectDocument(json("_id: 1, a: null"), json("'a.b': 1"), "_id"))
            .isEqualTo(json("_id: 1"));

        assertThat(Projection.projectDocument(json("_id: 1, a: {b: null}"), json("'a.b': 1"), "_id"))
            .isEqualTo(json("_id: 1, a: {b: null}"));

        assertThat(Projection.projectDocument(json("_id: 1, a: {b: null}"), json("'a.c': 1"), "_id"))
            .isEqualTo(json("_id: 1, a: {}"));
    }

    @Test
    public void testProjectListValues() throws Exception {
        assertThat(Projection.projectDocument(json("_id: 1, a: [1, 2, 3]"), json("'a.c': 1"), "_id"))
            .isEqualTo(json("_id: 1, a: []"));

        assertThat(Projection.projectDocument(json("_id: 1, a: [{x: 1}, 500, {y: 2}, {x: 3}]"), json("'a.x': 1"), "_id"))
            .isEqualTo(json("_id: 1, a: [{x: 1}, {}, {x: 3}]"));

        assertThat(Projection.projectDocument(json("_id: 1, a: [{x: 10, y: 100}, 100, {x: 20, y: 200}, {x: 3}, {z: 4}]"), json("'a.x': 1, 'a.y': 1"), "_id"))
            .isEqualTo(json("_id: 1, a: [{x: 10, y: 100}, {x: 20, y: 200}, {x: 3}, {}]"));

        assertThat(Projection.projectDocument(json("_id: 1, a: [100, {z: 4}, {y: 3}, {x: 1, y: 4}]"), json("'a.x': 1, 'a.y': 1"), "_id"))
            .isEqualTo(json("_id: 1, a: [{}, {y: 3}, {x: 1, y: 4}]"));
    }

    @Test
    public void testProjectWithElemMatch() throws Exception {
        Document document = json("_id: 1, students: [" +
            "{name: 'john', school: 'A', age: 10}, " +
            "{name: 'jess', school: 'B', age: 12}, " +
            "{name: 'jeff', school: 'A', age: 12}" +
            "]");

        assertThat(Projection.projectDocument(document, json("students: {$elemMatch: {school: 'B'}}"), "_id"))
            .isEqualTo(json("_id: 1, students: [{name: 'jess', school: 'B', age: 12}]"));

        assertThat(Projection.projectDocument(document, json("students: {$elemMatch: {school: 'A', age: {$gt: 10}}}"), "_id"))
            .isEqualTo(json("_id: 1, students: [{name: 'jeff', school: 'A', age: 12}]"));

        assertThat(Projection.projectDocument(document, json("students: {$elemMatch: {school: 'C'}}"), "_id"))
            .isEqualTo(json("_id: 1"));

        assertThat(Projection.projectDocument(json("_id: 1, students: [1, 2, 3]"), json("students: {$elemMatch: {school: 'C'}}"), "_id"))
            .isEqualTo(json("_id: 1"));

        assertThat(Projection.projectDocument(json("_id: 1, students: {school: 'C'}"), json("students: {$elemMatch: {school: 'C'}}"), "_id"))
            .isEqualTo(json("_id: 1"));

        assertThat(Projection.projectDocument(document, json("students: {$elemMatch: {school: 'A'}}"), "_id"))
            .isEqualTo(json("_id: 1, students: [{name: 'john', school: 'A', age: 10}]"));
    }

    @Test
    public void testProjectWithSlice() throws Exception {
        Document document = json("_id: 1, values: [1, 2, 3, 4], value: 'other'");

        assertThat(Projection.projectDocument(document, json("values: {$slice: 2}"), "_id"))
            .isEqualTo(json("_id: 1, values: [1, 2]"));

        assertThat(Projection.projectDocument(document, json("values: {$slice: 20}"), "_id"))
            .isEqualTo(json("_id: 1, values: [1, 2, 3, 4]"));

        assertThat(Projection.projectDocument(document, json("values: {$slice: -2}"), "_id"))
            .isEqualTo(json("_id: 1, values: [3, 4]"));

        assertThat(Projection.projectDocument(document, json("values: {$slice: -10}"), "_id"))
            .isEqualTo(json("_id: 1, values: [1, 2, 3, 4]"));

        assertThat(Projection.projectDocument(document, json("values: {$slice: [-2, 1]}"), "_id"))
            .isEqualTo(json("_id: 1, values: [3]"));

        assertThat(Projection.projectDocument(document, json("value: {$slice: 2}"), "_id"))
            .isEqualTo(json("_id: 1, value: 'other'"));
    }

}