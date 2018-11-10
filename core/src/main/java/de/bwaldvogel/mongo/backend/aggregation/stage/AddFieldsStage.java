package de.bwaldvogel.mongo.backend.aggregation.stage;

import java.util.Map.Entry;
import java.util.stream.Stream;

import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.backend.aggregation.Expression;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class AddFieldsStage implements AggregationStage {

    private final Document addFields;

    public AddFieldsStage(Document addFields) {
        if (addFields.isEmpty()) {
            throw new MongoServerError(40177, "Invalid $addFields :: caused by :: specification must have at least one field");
        }
        this.addFields = addFields;
    }

    @Override
    public Stream<Document> apply(Stream<Document> stream) {
        return stream.map(this::projectDocument);
    }

    Document projectDocument(Document document) {
        Document result = new Document();

        for (Entry<String, Object> entry : document.entrySet()) {
            String field = entry.getKey();
            if (addFields.containsKey(field)) {
                Object projectedValue = evaluate(addFields.get(field), document);
                result.put(field, projectedValue);
            } else {
                result.put(field, entry.getValue());
            }
        }

        for (Entry<String, Object> entry : addFields.entrySet()) {
            if (!result.containsKey(entry.getKey())) {
                Object projectedValue = evaluate(entry.getValue(), document);
                result.put(entry.getKey(), projectedValue);
            }
        }

        return result;
    }

    private static Object evaluate(Object value, Document document) {
        Object evaluatedValue = Expression.evaluate(value, document);
        if (evaluatedValue instanceof Missing) {
            return null;
        }
        return evaluatedValue;
    }

}
