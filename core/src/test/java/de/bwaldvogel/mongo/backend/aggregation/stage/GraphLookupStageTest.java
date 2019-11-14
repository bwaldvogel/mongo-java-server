package de.bwaldvogel.mongo.backend.aggregation.stage;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.TestUtils;
import de.bwaldvogel.mongo.bson.Document;
import org.junit.Before;
import org.junit.Test;

import java.util.stream.Stream;

import static de.bwaldvogel.mongo.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GraphLookupStageTest {

    MongoDatabase database;

    @SuppressWarnings("rawtypes")
    MongoCollection employeesCollection;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {
        database = mock(MongoDatabase.class);
        employeesCollection = mock(MongoCollection.class);
        when(database.resolveCollection("employees", false)).thenReturn(employeesCollection);
    }

    @Test
    public void testGraphLookupObjectThatHasLinkedDocuments() {
        GraphLookupStage graphLookupStage = buildGraphLookupStage("from: 'employees', 'startWith': '$manager', 'connectFromField': 'manager', 'connectToField': 'name', 'as': 'managers', 'depthField': 'order'");
        configureEmployeesCollection("name: 'Bob'", "_id: 3, name: 'Bob', manager: 'Dave'");
        configureEmployeesCollection("name: 'Dave'", "_id: 2, name: 'Dave', manager: 'Mike'");
        configureEmployeesCollection("name: 'Mike'", "_id: 1, name: 'Mike'");
        Document document = json("name: 'Bob', manager: 'Dave'");

        Stream<Document> result = graphLookupStage.apply(Stream.of(document));

        assertThat(result).containsExactly(
            json("manager: 'Dave', managers: [{_id: 1, name: 'Mike', 'order': 1}, {_id: 2, name: 'Dave', 'manager': 'Mike', 'order': 0}], name: 'Bob'")
        );
    }

    private GraphLookupStage buildGraphLookupStage(String jsonDocument) {
        return new GraphLookupStage(json(jsonDocument), database);
    }

    private void configureEmployeesCollection(String expectedJsonQuery, String... employees) {
        when(employeesCollection.handleQueryAsStream(json(expectedJsonQuery)))
            .thenAnswer(invocation -> Stream.of(employees).map(TestUtils::json));
    }

}
