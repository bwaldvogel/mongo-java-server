package de.bwaldvogel.mongo.backend.aggregation.stage;

import static de.bwaldvogel.mongo.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Ignore;
import org.junit.Test;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class UnsetStageTest {

	@Test
	public void testUnset() throws Exception {
		assertThat(unset("field1",
			json("_id: 1, field1: 'value1'"),
			json("_id: 2, field1: 'value1', field2: 'value2'"),
			json("_id: 3, field2: 'value2'"),
			json("_id: 4")))
			.containsExactly(
				json("_id: 1"),
				json("_id: 2, field2: 'value2'"),
				json("_id: 3, field2: 'value2'"),
				json("_id: 4")
			);
	}

	@Test
	public void testUnsetMultipleFields() throws Exception {
		assertThat(unset(Arrays.asList("field1", "field2"),
			json("_id: 1, field1: 'value1'"),
			json("_id: 2, field1: 'value1', field2: 'value2'"),
			json("_id: 3, field2: 'value2', field3: 'value3'"),
			json("_id: 4, field3: 'value3'"),
			json("_id: 5")))
			.containsExactly(
				json("_id: 1"),
				json("_id: 2"),
				json("_id: 3, field3: 'value3'"),
				json("_id: 4, field3: 'value3'"),
				json("_id: 5")
			);
	}

	@Test
	public void testUnsetWithSubdocument() throws Exception {
		assertThat(unset("fields.field1",
			json("_id: 1, fields: { field1: 'value1' }"),
			json("_id: 2, fields: { field1: 'value1', field2: 'value2' }"),
			json("_id: 3, fields: { field2: 'value2' }"),
			json("_id: 4, fields: { }"),
			json("_id: 5")))
			.containsExactly(
				json("_id: 1, fields: { }"),
				json("_id: 2, fields: { field2: 'value2' }"),
				json("_id: 3, fields: { field2: 'value2' }"),
				json("_id: 4, fields: { }"),
				json("_id: 5")
			);
	}

	private List<Document> unset(Object input, Document... documents) {
		return new UnsetStage(input).apply(Stream.of(documents)).collect(Collectors.toList());
	}

	@Ignore
	@Test
	public void testIllegalUnset() throws Exception {
		assertThatExceptionOfType(MongoServerError.class)
			.isThrownBy(() -> new UnsetStage(""))
			.withMessage("FIXME empty input");

		assertThatExceptionOfType(MongoServerError.class)
			.isThrownBy(() -> new UnsetStage("{}"))
			.withMessage("FIXME unsupported input type");

		assertThatExceptionOfType(MongoServerError.class)
			.isThrownBy(() -> new UnsetStage("[\"\"]"))
			.withMessage("FIXME empty input inside list");

		assertThatExceptionOfType(MongoServerError.class)
			.isThrownBy(() -> new UnsetStage("[{}]"))
			.withMessage("FIXME unsupported input type inside list");
	}

}
