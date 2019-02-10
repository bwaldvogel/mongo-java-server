package de.bwaldvogel.mongo.backend.aggregation.stage;

import static de.bwaldvogel.mongo.backend.Utils.describeType;

import java.util.stream.Stream;

import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.backend.aggregation.Expression;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class ReplaceRootStage implements AggregationStage {

    private final Object newRoot;

    public ReplaceRootStage(Document document) {
        newRoot = document.getOrMissing("newRoot");

        if (Missing.isNullOrMissing(newRoot)) {
            throw new MongoServerError(40231, "no newRoot specified for the $replaceRoot stage");
        }
    }

    @Override
    public Stream<Document> apply(Stream<Document> stream) {
        return stream.map(this::replaceRoot);
    }

    Document replaceRoot(Document document) {
        Object result = Expression.evaluateDocument(newRoot, document);

        if (!(result instanceof Document)) {
            throw typeMismatchError(result, document);
        }

        return (Document) result;
    }

    private static MongoServerError typeMismatchError(Object subject, Document document) {
        return new MongoServerError(40228, "'newRoot' expression must evaluate to an object, but resulting value was: " + serializeType(subject)
            + ". Type of resulting value: '" + describeType(subject)
            + "'. Input document: " + document.toString(true)); // TODO: Mongo will only show the matched element (if any). How to get it?
    }

    private static String serializeType(Object subject) {
        if (Missing.class.isAssignableFrom(subject.getClass())) {
            return "MISSING";
        }
        if (String.class.isAssignableFrom(subject.getClass())) {
            return "\"" + subject + "\"";
        }
        // TODO: how to serialize other types e.g. BIN_DATA?
        return subject.toString();
    }
}
