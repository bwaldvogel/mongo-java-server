package de.bwaldvogel.mongo.backend.aggregation.stage;

import static de.bwaldvogel.mongo.backend.Utils.describeType;

import java.util.Collection;
import java.util.stream.Stream;

import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class UnwindStage implements AggregationStage {

    private final String path;

    public UnwindStage(Object input) {
        if (!(input instanceof String) && !(input instanceof Document)) {
            throw new MongoServerError(15981,
                "expected either a string or an object as specification for $unwind stage, got " + describeType(input));
        }

        final String fieldPath;
        if (input instanceof Document) {
            Document inputDocument = (Document) input;
            if (!inputDocument.containsKey("path")) {
                throw new MongoServerError(28812, "no path specified to $unwind stage");
            }

            Object path= inputDocument.get("path");
            if (!(path instanceof String)) {
                throw new MongoServerError(28808, "expected a string as the path for $unwind stage, got " + describeType(path));
            }
            fieldPath = (String) path;
        } else {
            fieldPath = (String) input;
        }
        if (!fieldPath.startsWith("$")) {
            throw new MongoServerError(28818, "path option to $unwind stage should be prefixed with a '$': " + fieldPath);
        }
        this.path = fieldPath.substring(1);
    }

    @Override
    public Stream<Document> apply(Stream<Document> stream) {
        return stream.flatMap(document -> {
            Object values = Utils.getSubdocumentValue(document, path);
            if (Missing.isNullOrMissing(values)) {
                return Stream.empty();
            }
            Collection<?> collection = (Collection<?>) values;
            return collection.stream()
                .map(collectionValue -> {
                    Document documentClone = document.cloneDeeply();
                    Utils.changeSubdocumentValue(documentClone, path, collectionValue);
                    return documentClone;
                });
        });
    }

}
