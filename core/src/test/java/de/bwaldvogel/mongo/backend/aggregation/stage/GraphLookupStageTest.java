package de.bwaldvogel.mongo.backend.aggregation.stage;

import static de.bwaldvogel.mongo.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.mockito.Mockito.when;

import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.TestUtils;
import de.bwaldvogel.mongo.bson.Document;

class GraphLookupStageTest extends AbstractLookupStageTest {

    @Mock
    @SuppressWarnings("rawtypes")
    private MongoCollection employeesCollection;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        when(database.resolveCollection("employees", false)).thenReturn(employeesCollection);
    }

    @Test
    void testGraphLookupObjectThatHasLinkedDocuments() {
        GraphLookupStage graphLookupStage = buildGraphLookupStage("from: 'employees', 'startWith': '$manager', 'connectFromField': 'manager', 'connectToField': 'name', 'as': 'managers', 'depthField': 'order'");
        configureEmployeesCollection("name: 'Dave'", "_id: 2, name: 'Dave', manager: 'Mike'");
        configureEmployeesCollection("name: 'Mike'", "_id: 1, name: 'Mike'");
        Document document = json("name: 'Bob', manager: 'Dave'");

        Document result = graphLookupStage.apply(Stream.of(document)).findFirst().orElseThrow();

        assertThat(result).containsEntry("name", "Bob").containsEntry("manager", "Dave");
        assertThat(result.get("managers")).asInstanceOf(list(Document.class)).containsExactlyInAnyOrder(
            json("_id: 1, name: 'Mike', order: NumberLong(1)"),
            json("_id: 2, name: 'Dave', manager: 'Mike', order: NumberLong(0)")
        );
    }

    @Test
    void testGraphLookupDoesNotModifyInputOrLinkedDocuments() {
        GraphLookupStage graphLookupStage = buildGraphLookupStage("""
            from: 'employees',
            startWith: '$manager',
            connectFromField: 'manager',
            connectToField: 'name',
            as: 'managers',
            depthField: 'order'
            """);
        Document employee = json("_id: 2, name: 'Dave', order: 42");
        when(employeesCollection.handleQueryAsStream(json("name: 'Dave'")))
            .thenAnswer(invocation -> Stream.of(employee));
        Document input = json("name: 'Bob', manager: 'Dave', managers: ['previous value']");

        assertThat(graphLookupStage.apply(Stream.of(input))).containsExactly(
            json("name: 'Bob', manager: 'Dave', managers: [{_id: 2, name: 'Dave', order: NumberLong(0)}]")
        );
        assertThat(input).isEqualTo(json("name: 'Bob', manager: 'Dave', managers: ['previous value']"));
        assertThat(employee).isEqualTo(json("_id: 2, name: 'Dave', order: 42"));
    }

    private GraphLookupStage buildGraphLookupStage(String jsonDocument) {
        return new GraphLookupStage(json(jsonDocument), database);
    }

    private void configureEmployeesCollection(String expectedJsonQuery, String... employees) {
        when(employeesCollection.handleQueryAsStream(json(expectedJsonQuery)))
            .thenAnswer(invocation -> Stream.of(employees).map(TestUtils::json));
    }

}
