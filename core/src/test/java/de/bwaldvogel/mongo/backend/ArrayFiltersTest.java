package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.entry;

import java.util.List;

import org.junit.Test;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.BadValueException;
import de.bwaldvogel.mongo.exception.FailedToParseException;

public class ArrayFiltersTest {

    @Test
    public void testParseAndCalculateKeys_Simple() throws Exception {
        Document query = json("arrayFilters: [{x: {$gt: 20}}]");
        Document updateQuery = json("$set: {'values.$[x]': 20}");

        ArrayFilters arrayFilters = ArrayFilters.parse(query, updateQuery);

        List<String> keys = arrayFilters.calculateKeys(json("values: [10, 30, 20, 40]"), "values.$[x]");
        assertThat(keys).containsExactly("values.1", "values.3");
    }

    @Test
    public void testParseAndCalculateKeys_Subdocument() throws Exception {
        Document query = json("arrayFilters: [{'elem.name': {$in: ['A', 'B']}}]");
        Document updateQuery = json("$set: {'values.$[elem].active': true}");

        ArrayFilters arrayFilters = ArrayFilters.parse(query, updateQuery);

        assertThat(arrayFilters.getValues())
            .containsExactly(entry("elem", json("name: {$in: ['A', 'B']}")));

        List<String> keys = arrayFilters.calculateKeys(
            json("values: [{name: 'A'}, {name: 'B'}, {name: 'C'}]"),
            "values.$[elem].active");
        assertThat(keys).containsExactly("values.0.active", "values.1.active");
    }

    @Test
    public void testParseAndCalculateKeys_NestedSubdocument() throws Exception {
        Document query = json("arrayFilters: [{'elem.name': {$in: ['B', 'C']}}]");
        Document updateQuery = json("$set: {'a.b.$[elem].active': true}");

        ArrayFilters arrayFilters = ArrayFilters.parse(query, updateQuery);

        assertThat(arrayFilters.getValues())
            .containsExactly(entry("elem", json("name: {$in: ['B', 'C']}")));

        List<String> keys = arrayFilters.calculateKeys(
            json("a: {b: [{name: 'A'}, {name: 'B'}, {name: 'C'}]}"),
            "a.b.$[elem].active");
        assertThat(keys).containsExactly("a.b.1.active", "a.b.2.active");
    }

    @Test
    public void testParseAndCalculateKeys_PathDoesNotExist() throws Exception {
        Document query = json("arrayFilters: [{x: {$gt: 20}}]");
        Document updateQuery = json("$set: {'a.b.$[x]': 20}");

        ArrayFilters arrayFilters = ArrayFilters.parse(query, updateQuery);

        assertThatExceptionOfType(BadValueException.class)
            .isThrownBy(() -> arrayFilters.calculateKeys(json("a: [10, 30, 20, 40]"), "a.b.$[x]"))
            .withMessage("[Error 2] The path 'a.b' must exist in the document in order to apply array updates.");
    }

    @Test
    public void testParseAndCalculateKeys_TopLevelPathDoesNotExist() throws Exception {
        Document query = json("arrayFilters: [{x: {$gt: 20}}]");
        Document updateQuery = json("$set: {'a.b.$[x]': 20}");

        ArrayFilters arrayFilters = ArrayFilters.parse(query, updateQuery);

        assertThatExceptionOfType(BadValueException.class)
            .isThrownBy(() -> arrayFilters.calculateKeys(json("b: 123"), "a.b.$[x]"))
            .withMessage("[Error 2] The path 'a.b' must exist in the document in order to apply array updates.");
    }

    @Test
    public void testParseAndCalculateKeys_NonArray() throws Exception {
        Document query = json("arrayFilters: [{x: {$gt: 20}}]");
        Document updateQuery = json("$set: {'a.b.$[x]': 20}");

        ArrayFilters arrayFilters = ArrayFilters.parse(query, updateQuery);

        assertThatExceptionOfType(BadValueException.class)
            .isThrownBy(() -> arrayFilters.calculateKeys(json("a: {b: 10}"), "a.b.$[x]"))
            .withMessage("[Error 2] Cannot apply array updates to non-array element b: 10");
    }

    @Test
    public void testParseAndCalculateKeys_PositionalAll() throws Exception {
        Document query = json("");
        Document updateQuery = json("$set: {'values.$[].active': true}");

        ArrayFilters arrayFilters = ArrayFilters.parse(query, updateQuery);

        List<String> keys = arrayFilters.calculateKeys(
            json("values: [{name: 'A'}, {name: 'B'}, {name: 'C'}]"),
            "values.$[].active");
        assertThat(keys).containsExactly("values.0.active", "values.1.active", "values.2.active");
    }

    @Test
    public void testParseAndCalculateKeys_PositionalAllAndElementFilter() throws Exception {
        Document query = json("arrayFilters: [{element: {$gte: 3}}]");
        Document updateQuery = json("$inc: {'grades.$[].x.$[element]': 1}");

        ArrayFilters arrayFilters = ArrayFilters.parse(query, updateQuery);

        List<String> keys = arrayFilters.calculateKeys(
            json("grades: [{x: [1, 2, 3]}, {x: [3, 4, 5]}, {x: [1, 2, 3]}]"),
            "grades.$[].x.$[element]");
        assertThat(keys).containsExactly(
            "grades.0.x.2",
            "grades.1.x.0",
            "grades.1.x.1",
            "grades.1.x.2",
            "grades.2.x.2"
        );
    }

    @Test
    public void testParse_FilterNotUsed() throws Exception {
        Document query = json("arrayFilters: [{x: {$gt: 20}}]");
        Document updateQuery = json("$set: {'a.b': 20}");

        assertThatExceptionOfType(FailedToParseException.class)
            .isThrownBy(() -> ArrayFilters.parse(query, updateQuery))
            .withMessage("[Error 9] The array filter for identifier 'x' was not used in the update { $set: { a.b: 20 } }");
    }

    @Test
    public void testParseAndCalculateKeys_TopLevelFilter() throws Exception {
        Document query = json("arrayFilters: [{x: {$gt: 20}}]");
        Document updateQuery = json("$set: {'$[x]': 20}");

        ArrayFilters arrayFilters = ArrayFilters.parse(query, updateQuery);

        assertThatExceptionOfType(BadValueException.class)
            .isThrownBy(() -> arrayFilters.calculateKeys(json("a: {b: 10}"), "$[x]"))
            .withMessage("[Error 2] Cannot have array filter identifier (i.e. '$[<id>]') element in the first position in path '$[x]'");
    }

    @Test
    public void testParse_MultipleFilters() throws Exception {
        Document query = json("arrayFilters: [{x: {$gt: 20}}, {y: {$lt: 10}}]");
        Document updateQuery = json("$set: {'values.$[x]': 20, 'values.$[y]': 30}");

        ArrayFilters arrayFilters = ArrayFilters.parse(query, updateQuery);

        assertThat(arrayFilters.getValues())
            .containsExactly(
                entry("x", json("$gt: 20")),
                entry("y", json("$lt: 10")));
    }

}
