package de.bwaldvogel.mongo.backend.aggregation.stage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class UnsetStage implements AggregationStage {

    private List<String> unsetPaths = new ArrayList<>();

    public UnsetStage(Object input) {
        if (input instanceof String) {
            unsetPaths.add((String) input);
        } else if (input instanceof Collection<?>) {
            for (Object fieldPath : (Collection<?>) input) {
                if (!(fieldPath instanceof String)) {
                    throw mustBeStringOrStringArrayError();
                }
                unsetPaths.add((String) fieldPath);
            }
        } else {
            throw mustBeStringOrStringArrayError();
        }
    }

    private static MongoServerError mustBeStringOrStringArrayError() {
        return new MongoServerError(31120, "$unset specification must be a string or an array containing only string values");
    }

    @Override
    public Stream<Document> apply(Stream<Document> stream) {
        return stream.map(this::unsetDocumentFields);
    }

    Document unsetDocumentFields(Document document) {
        Document result = document.cloneDeeply();
        for (String unsetPath : unsetPaths) {
            Utils.removeSubdocumentValue(result, unsetPath);
        }
        return result;
    }

}
