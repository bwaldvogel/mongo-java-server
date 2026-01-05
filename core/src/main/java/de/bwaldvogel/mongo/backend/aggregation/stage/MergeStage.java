package de.bwaldvogel.mongo.backend.aggregation.stage;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.AbstractMongoCollection.FindAndModifyPlanExecutorError;
import de.bwaldvogel.mongo.backend.Assert;
import de.bwaldvogel.mongo.backend.CollectionUtils;
import de.bwaldvogel.mongo.backend.DatabaseResolver;
import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.backend.IndexKey;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.backend.aggregation.Aggregation;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.BadValueException;
import de.bwaldvogel.mongo.exception.ImmutableFieldException;
import de.bwaldvogel.mongo.exception.InvalidOptionsException;
import de.bwaldvogel.mongo.exception.MergeStageNoMatchingDocumentException;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.TypeMismatchException;

public class MergeStage extends TerminalStage {

    private static final Set<String> KNOWN_KEYS = Set.of("into", "on", "let", "whenMatched", "whenNotMatched");
    private static final Set<Class<?>> ALLOWED_STAGES_IN_PIPELINE = Set.of(
        AddFieldsStage.class,
        ProjectStage.class,
        UnsetStage.class,
        ReplaceRootStage.class
    );

    private final Supplier<MongoCollection<?>> targetCollectionSupplier;
    private final Set<String> joinFields;
    private final Map<String, Object> let;
    private final WhenMatched whenMatched;
    private Aggregation whenMatchedPipeline = null;
    private final WhenNotMatched whenNotMatched;

    private enum WhenMatched {
        replace,
        keepExisting,
        merge,
        fail
    }

    private enum WhenNotMatched {
        insert,
        discard,
        fail
    }

    public MergeStage(DatabaseResolver databaseResolver, MongoDatabase database, Object params) {
        if (params instanceof String) {
            params = new Document("into", params);
        }
        Document paramsDocument = (Document) params;
        for (String key : paramsDocument.keySet()) {
            if (!KNOWN_KEYS.contains(key)) {
                throw new MongoServerError(40415, "BSON field '$merge." + key + "' is an unknown field.");
            }
        }

        targetCollectionSupplier = getTargetCollectionSupplier(databaseResolver, database, paramsDocument);

        joinFields = getJoinFields(paramsDocument);
        if (!hasUniqueIndexOnJoinFields()) {
            throw new MongoServerError(51183, "Cannot find index to verify that join fields will be unique");
        }

        let = getLet(paramsDocument);
        whenMatched = getWhenMatched(paramsDocument);
        whenNotMatched = getWhenNotMatched(paramsDocument);

        if (whenMatched == null) {
            Collection<Document> pipeline = (Collection<Document>) paramsDocument.get("whenMatched");
            whenMatchedPipeline = Aggregation.fromPipeline(pipeline, databaseResolver, database, null, null);
        } else if (paramsDocument.containsKey("let")) {
            throw new MongoServerError(51199, "Cannot use 'let' variables with 'whenMatched: " + whenMatched + "' mode");
        }
    }

    private Map<String, Object> getLet(Document paramsDocument) {
        Object let = paramsDocument.get("let");
        if (let == null) {
            return new Document("$new", "$$ROOT");
        }

        if (!(let instanceof Document)) {
            throw new TypeMismatchException("BSON field '$merge.let' is the wrong type '" + Utils.describeType(let) + "', expected type 'object'");
        }

        Map<String, Object> variables = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : ((Document) let).entrySet()) {
            if (entry.getKey().equals("new") && !entry.getValue().equals("$$ROOT")) {
                throw new MongoServerError(51273, "'let' may not define a value for the reserved 'new' variable other than '$$ROOT'");
            }
            variables.put("$" + entry.getKey(), entry.getValue());
        }
        return variables;
    }

    private static Supplier<MongoCollection<?>> getTargetCollectionSupplier(DatabaseResolver databaseResolver,
                                                                            MongoDatabase database,
                                                                            Document paramsDocument) {
        Object into = paramsDocument.get("into");
        if (into instanceof String collectionName) {
            return () -> resolveOrCreateCollection(database, collectionName);
        } else if (into instanceof Document intoDocument) {
            for (String intoKey : intoDocument.keySet()) {
                if (!intoKey.equals("db") && !intoKey.equals("coll")) {
                    throw new MongoServerError(40415, "BSON field 'into." + intoKey + "' is an unknown field.");
                }
            }
            String collectionName = (String) intoDocument.get("coll");
            return () -> {
                String databaseName = (String) intoDocument.get("db");
                MongoDatabase resolvedDatabase = databaseResolver.resolve(databaseName);
                return resolveOrCreateCollection(resolvedDatabase, collectionName);
            };
        } else {
            throw new MongoServerError(51178, "$merge 'into' field  must be either a string or an object, but found " + Utils.describeType(into));
        }
    }

    private boolean hasUniqueIndexOnJoinFields() {
        return targetCollectionSupplier.get().getIndexes()
            .stream()
            .filter(Index::isUnique)
            .anyMatch(this::matchesJoinFields);
    }

    private boolean matchesJoinFields(Index<?> index) {
        Set<String> indexKeys = index.getKeys().stream()
            .map(IndexKey::getKey)
            .collect(Collectors.toSet());
        return indexKeys.equals(joinFields);
    }

    private Set<String> getJoinFields(Document paramsDocument) {
        Object on = paramsDocument.getOrDefault("on", "_id");
        if (on instanceof String string) {
            return Set.of(string);
        }
        if (on instanceof Collection<?> collection) {
            if (collection.isEmpty()) {
                throw new MongoServerError(51187, "If explicitly specifying $merge 'on', must include at least one field");
            }
            Set<String> joinFields = new LinkedHashSet<>();
            for (Object value : collection) {
                if (!(value instanceof String)) {
                    throw new MongoServerError(51134, "$merge 'on' array elements must be strings, but found " + Utils.describeType(value));
                }
                String joinField = (String) value;
                if (!joinFields.add(joinField)) {
                    throw new MongoServerError(31465, "Found a duplicate field '" + joinField + "'");
                }
            }
            return joinFields;
        } else {
            throw new MongoServerError(51186, "$merge 'on' field  must be either a string or an array of strings, but found " + Utils.describeType(on));
        }
    }

    private WhenMatched getWhenMatched(Document paramsDocument) {
        Object whenMatched = paramsDocument.getOrDefault("whenMatched", WhenMatched.merge.name());
        if (whenMatched instanceof String matched) {
            try {
                return WhenMatched.valueOf(matched);
            } catch (IllegalArgumentException e) {
                throw new BadValueException("Enumeration value '" + whenMatched + "' for field 'whenMatched' is not a valid value.");
            }
        } else if (whenMatched instanceof Collection<?> pipeline) {
            for (Object pipelineElement : pipeline) {
                if (!(pipelineElement instanceof Document)) {
                    throw new TypeMismatchException("Each element of the 'pipeline' array must be an object");
                }
            }
            return null;
        } else {
            throw new MongoServerError(51191, "$merge 'whenMatched' field  must be either a string or an array, but found " + Utils.describeType(whenMatched));
        }
    }

    private WhenNotMatched getWhenNotMatched(Document paramsDocument) {
        Object whenNotMatched = paramsDocument.getOrDefault("whenNotMatched", WhenNotMatched.insert.name());
        if (!(whenNotMatched instanceof String)) {
            throw new TypeMismatchException("BSON field '$merge.whenNotMatched' is the wrong type '" + Utils.describeType(whenNotMatched) + "', expected type 'string'");
        }

        try {
            return WhenNotMatched.valueOf((String) whenNotMatched);
        } catch (IllegalArgumentException e) {
            throw new BadValueException("Enumeration value '" + whenNotMatched + "' for field '$merge.whenNotMatched' is not a valid value.");
        }
    }

    @Override
    public String name() {
        return "$merge";
    }

    @Override
    public void applyLast(Stream<Document> stream) {
        MongoCollection<?> collection = targetCollectionSupplier.get();

        validateWhenMatchedPipeline();

        stream.forEach(document -> {
            Document query = getJoinQuery(document);
            Optional<Document> matchingDocument = collection.handleQueryAsStream(query).findFirst();
            if (matchingDocument.isPresent()) {
                Document existingDocument = matchingDocument.get();
                if (whenMatchedPipeline != null) {
                    let.put("$ROOT", document);
                    whenMatchedPipeline.setVariables(let);
                    List<Document> pipelineOutput = whenMatchedPipeline.runStages(Stream.of(existingDocument));
                    if (!pipelineOutput.isEmpty()) {
                        replaceDocument(collection, existingDocument, CollectionUtils.getSingleElement(pipelineOutput));
                    }
                } else {
                    switch (whenMatched) {
                        case merge:
                            Document mergedDocument = existingDocument.clone();
                            mergedDocument.merge(document);
                            assertIdHasNotChanged(existingDocument, mergedDocument);
                            replaceDocument(collection, existingDocument, mergedDocument);
                            break;
                        case replace:
                            replaceDocument(collection, existingDocument, document);
                            break;
                        case fail:
                            // this triggers a DuplicateKeyError
                            collection.addDocument(document);
                            break;
                        case keepExisting:
                            break;
                        default:
                            throw new UnsupportedOperationException("whenMatched '" + whenMatched + "' is not yet implemented");
                    }
                }
            } else {
                switch (whenNotMatched) {
                    case insert:
                        collection.addDocument(document);
                        break;
                    case discard:
                        break;
                    case fail:
                        throw new MergeStageNoMatchingDocumentException();
                    default:
                        throw new UnsupportedOperationException("whenNotMatched '" + whenNotMatched + "' is not yet implemented");
                }
            }
        });
    }

    private void validateWhenMatchedPipeline() {
        if (whenMatchedPipeline == null) {
            return;
        }
        for (AggregationStage stage : whenMatchedPipeline.getStages()) {
            if (!ALLOWED_STAGES_IN_PIPELINE.contains(stage.getClass())) {
                throw new InvalidOptionsException(stage.name() + " is not allowed to be used within an update");
            }
        }
    }

    private static void assertIdHasNotChanged(Document one, Document other) {
        if (!one.get("_id").equals(other.get("_id"))) {
            throw new ImmutableFieldException("$merge failed to update the matching document, did you attempt to modify the _id or the shard key?" +
                " :: caused by :: " +
                "Performing an update on the path '_id' would modify the immutable field '_id'");
        }
    }

    private Document getJoinQuery(Document document) {
        Document query = new Document();
        for (String field : this.joinFields) {
            query.put(field, document.get(field));
        }
        return query;
    }

    private void replaceDocument(MongoCollection<?> collection, Document existingDocument, Document document) {
        Document documentSelector = new Document("_id", existingDocument.get("_id"));
        try {
            Document result = collection.findAndModify(new Document("query", documentSelector)
                .append("new", false)
                .append("upsert", false)
                .append("update", document));
            Assert.equals(result.get("ok"), 1.0);
        } catch (FindAndModifyPlanExecutorError e) {
            MongoServerError cause = e.getCause();
            throw new MongoServerError(cause.getCode(), cause.getCodeName(),
                "$merge failed to update the matching document,"
                    + " did you attempt to modify the _id or the shard key? :: caused by :: "
                    + cause.getMessageWithoutErrorCode(), cause);
        }
    }

    private static MongoCollection<?> resolveOrCreateCollection(MongoDatabase database, String collectionName) {
        MongoCollection<?> collection = database.resolveCollection(collectionName, false);
        if (collection == null) {
            collection = database.createCollectionOrThrowIfExists(collectionName);
        }
        return collection;
    }

    @Override
    public boolean isModifying() {
        return true;
    }
}
