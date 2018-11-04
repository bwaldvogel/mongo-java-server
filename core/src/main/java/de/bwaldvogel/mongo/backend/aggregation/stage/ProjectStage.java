package de.bwaldvogel.mongo.backend.aggregation.stage;

import static de.bwaldvogel.mongo.backend.Constants.ID_FIELD;

import java.util.Map;
import java.util.stream.Stream;

import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.backend.aggregation.Expression;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class ProjectStage implements AggregationStage {

    private final Document projection;

    public ProjectStage(Document projection) {
        this.projection = projection;
    }

    @Override
    public Stream<Document> apply(Stream<Document> stream) {
        return stream.map(document -> projectDocument(document, projection));
    }

    static Document projectDocument(Document input, Document projection) {
        if (projection == null) {
            return input;
        }
        if (projection.isEmpty()) {
            throw new MongoServerError(40177, "Invalid $project :: caused by :: specification must have at least one field");
        }
        Document result = new Document();

        if (!projection.containsKey(ID_FIELD)) {
            putIfContainsField(input, result, ID_FIELD);
        }

        for (Map.Entry<String, Object> entry : projection.entrySet()) {
            String field = entry.getKey();
            Object projectionValue = entry.getValue();
            if (projectionValue instanceof Number || projectionValue instanceof Boolean) {
                if (Utils.isTrue(projectionValue)) {
                    putIfContainsField(input, result, field);
                }
            } else if (projectionValue == null) {
                result.put(field, null);
            } else {
                Object projectedValue = Expression.evaluate(projectionValue, input);
                if (projectedValue != null) {
                    result.put(field, projectedValue);
                }
            }
        }
        return result;
    }

    private static void putIfContainsField(Document input, Document result, String field) {
        if (input.containsKey(field)) {
            result.put(field, input.get(field));
        }
    }
}
