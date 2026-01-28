package de.bwaldvogel.mongo.backend.aggregation.stage;

import static de.bwaldvogel.mongo.backend.Constants.ID_FIELD;

import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.backend.aggregation.Expression;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.BadValueException;
import de.bwaldvogel.mongo.exception.ConversionFailureException;
import de.bwaldvogel.mongo.exception.ErrorCode;
import de.bwaldvogel.mongo.exception.FailedToOptimizePipelineError;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class ProjectStage implements AggregationStage {

    private final Document projection;
    private final boolean hasInclusions;

    public ProjectStage(Object projection) {
        if (!(projection instanceof Document)) {
            throw new MongoServerError(15969, "$project specification must be an object");
        }
        Document projectionSpecification = (Document) projection;
        if (projectionSpecification.isEmpty()) {
            throw new MongoServerError(40177, "specification must have at least one field");
        }
        this.projection = projectionSpecification;
        this.hasInclusions = hasInclusions(projectionSpecification);
        validateProjection();
    }

    @Override
    public String name() {
        return "$project";
    }

    private static boolean hasInclusions(Document projection) {
        return projection.values().stream().anyMatch(Utils::isTrue);
    }

    @Override
    public Stream<Document> apply(Stream<Document> stream) {
        return stream.map(this::projectDocument);
    }

    Document projectDocument(Document document) {
        try {
            final Document result;
            if (hasInclusions) {
                result = new Document();
                if (!projection.containsKey(ID_FIELD)) {
                    Utils.copySubdocumentValue(document, result, ID_FIELD);
                }
            } else {
                result = document.cloneDeeply();
            }

            for (Entry<String, Object> entry : projection.entrySet()) {
                String field = entry.getKey();
                Object projectionValue = entry.getValue();
                if (isNumberOrBoolean(projectionValue)) {
                    if (Utils.isTrue(projectionValue)) {
                        Utils.copySubdocumentValue(document, result, field);
                    } else {
                        Utils.removeSubdocumentValue(result, field);
                    }
                } else if (projectionValue instanceof List) {
                    List<Object> resolvedProjectionValues = ((List<?>) projectionValue)
                        .stream()
                        .map(value -> Expression.evaluateDocument(value, document))
                        .collect(Collectors.toList());
                    Utils.changeSubdocumentValue(result, field, resolvedProjectionValues);
                } else if (projectionValue == null) {
                    Utils.changeSubdocumentValue(result, field, null);
                } else {
                    Object value = Expression.evaluateDocument(projectionValue, document);
                    if (!(value instanceof Missing)) {
                        Utils.changeSubdocumentValue(result, field, value);
                    }
                }
            }
            return result;
        } catch (ConversionFailureException | FailedToOptimizePipelineError | BadValueException e) {
            throw e;
        } catch (MongoServerError e) {
            if (e.hasCode(ErrorCode._34471, ErrorCode._40390)) {
                throw e;
            }
            throw new InvalidProjectException(e);
        }
    }

    public static class InvalidProjectException extends MongoServerError {

        private static final long serialVersionUID = 1L;

        private static final String MESSAGE_PREFIX = "Invalid $project :: caused by :: ";

        private InvalidProjectException(MongoServerError cause) {
            super(cause.getCode(), cause.getCodeName(), MESSAGE_PREFIX + cause.getMessageWithoutErrorCode(), cause);
        }
    }

    private void validateProjection() {
        if (hasInclusions) {
            projection.entrySet().stream()
                .filter(entry -> !entry.getKey().equals(ID_FIELD))
                .filter(entry -> isNumberOrBoolean(entry.getValue()))
                .filter(entry -> !Utils.isTrue(entry.getValue()))
                .findFirst()
                .ifPresent(nonIdExclusion -> {
                    throw new MongoServerError(31254,
                        "Invalid $project :: caused by :: Cannot do exclusion on field " + nonIdExclusion.getKey() + " in inclusion projection");
                });
        }
    }

    private static boolean isNumberOrBoolean(Object projectionValue) {
        return projectionValue instanceof Number || projectionValue instanceof Boolean;
    }

}
