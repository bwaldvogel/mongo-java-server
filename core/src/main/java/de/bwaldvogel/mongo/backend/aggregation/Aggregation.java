package de.bwaldvogel.mongo.backend.aggregation;

import static de.bwaldvogel.mongo.backend.Constants.ID_FIELD;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.Assert;
import de.bwaldvogel.mongo.backend.CollectionUtils;
import de.bwaldvogel.mongo.backend.DatabaseResolver;
import de.bwaldvogel.mongo.backend.aggregation.stage.AddFieldsStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.AggregationStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.BucketStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.FacetStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.GraphLookupStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.GroupStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.IndexStatsStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.LimitStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.LookupStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.LookupWithPipelineStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.MatchStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.MergeStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.OrderByStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.OutStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.ProjectStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.RedactStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.ReplaceRootStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.SampleStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.SkipStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.TerminalStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.UnsetStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.UnwindStage;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.ErrorCode;
import de.bwaldvogel.mongo.exception.FailedToOptimizePipelineError;
import de.bwaldvogel.mongo.exception.FailedToParseException;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerNotYetImplementedException;
import de.bwaldvogel.mongo.exception.TypeMismatchException;
import de.bwaldvogel.mongo.oplog.Oplog;

public class Aggregation {

    private static final Logger log = LoggerFactory.getLogger(Aggregation.class);

    private final MongoCollection<?> collection;

    private final List<AggregationStage> stages = new ArrayList<>();
    private Map<String, Object> variables = Collections.emptyMap();

    private Aggregation(MongoCollection<?> collection) {
        this.collection = collection;
    }

    public static List<Document> parse(Object pipelineObject) {
        if (!(pipelineObject instanceof List)) {
            throw new TypeMismatchException("'pipeline' option must be specified as an array");
        }
        List<Document> pipeline = new ArrayList<>();
        for (Object pipelineElement : (List<?>) pipelineObject) {
            if (!(pipelineElement instanceof Document)) {
                throw new TypeMismatchException("Each element of the 'pipeline' array must be an object");
            }
            pipeline.add((Document) pipelineElement);
        }
        return pipeline;
    }

    public static Aggregation fromPipeline(Object pipelineObject, DatabaseResolver databaseResolver,
                                           MongoDatabase database, MongoCollection<?> collection, Oplog oplog) {
        List<Document> pipeline = parse(pipelineObject);
        return fromPipeline(pipeline, databaseResolver, database, collection, oplog);
    }

    public static Aggregation fromPipeline(List<Document> pipeline, DatabaseResolver databaseResolver,
                                           MongoDatabase database, MongoCollection<?> collection, Oplog oplog) {
        Aggregation aggregation = new Aggregation(collection);

        for (Document stage : pipeline) {
            String stageOperation = CollectionUtils.getSingleElement(stage.keySet(), () -> {
                throw new MongoServerError(40323, "A pipeline stage specification object must contain exactly one field.");
            });
            switch (stageOperation) {
                case "$match":
                    Document matchQuery = (Document) stage.get(stageOperation);
                    aggregation.addStage(new MatchStage(matchQuery));
                    break;
                case "$skip":
                    Number numSkip = (Number) stage.get(stageOperation);
                    aggregation.addStage(new SkipStage(numSkip.longValue()));
                    break;
                case "$limit":
                    Number numLimit = (Number) stage.get(stageOperation);
                    aggregation.addStage(new LimitStage(numLimit.longValue()));
                    break;
                case "$sort":
                    Document orderBy = (Document) stage.get(stageOperation);
                    aggregation.addStage(new OrderByStage(orderBy));
                    break;
                case "$project":
                    Document projection = (Document) stage.get(stageOperation);
                    aggregation.addStage(new ProjectStage(projection));
                    break;
                case "$count":
                    String count = (String) stage.get(stageOperation);
                    aggregation.addStage(new GroupStage(new Document(ID_FIELD, null).append(count, new Document("$sum", 1))));
                    aggregation.addStage(new ProjectStage(new Document(ID_FIELD, 0)));
                    break;
                case "$group":
                    Document groupDetails = (Document) stage.get(stageOperation);
                    aggregation.addStage(new GroupStage(groupDetails));
                    break;
                case "$addFields":
                    Document addFieldsDetails = (Document) stage.get(stageOperation);
                    aggregation.addStage(new AddFieldsStage(addFieldsDetails));
                    break;
                case "$unwind":
                    Object unwind = stage.get(stageOperation);
                    aggregation.addStage(new UnwindStage(unwind));
                    break;
                case "$graphLookup":
                    Document graphLookup = (Document) stage.get(stageOperation);
                    aggregation.addStage(new GraphLookupStage(graphLookup, database));
                    break;
                case "$lookup":
                    Document lookup = (Document) stage.get(stageOperation);
                    if (lookup.containsKey(LookupWithPipelineStage.PIPELINE_FIELD)) {
                        aggregation.addStage(new LookupWithPipelineStage(lookup, database, databaseResolver, oplog));
                    } else {
                        aggregation.addStage(new LookupStage(lookup, database));
                    }
                    break;
                case "$replaceRoot":
                    Document replaceRoot = (Document) stage.get(stageOperation);
                    aggregation.addStage(new ReplaceRootStage(replaceRoot));
                    break;
                case "$sample":
                    Object sample = stage.get(stageOperation);
                    aggregation.addStage(new SampleStage(sample));
                    break;
                case "$sortByCount":
                    Object expression = stage.get(stageOperation);
                    aggregation.addStage(new GroupStage(new Document(ID_FIELD, expression).append("count", new Document("$sum", 1))));
                    aggregation.addStage(new OrderByStage(new Document("count", -1).append(ID_FIELD, 1)));
                    break;
                case "$bucket":
                    Document bucket = (Document) stage.get(stageOperation);
                    aggregation.addStage(new BucketStage(bucket));
                    break;
                case "$facet":
                    Document facet = (Document) stage.get(stageOperation);
                    aggregation.addStage(new FacetStage(facet, databaseResolver, database, collection, oplog));
                    break;
                case "$unset":
                    Object unset = stage.get(stageOperation);
                    aggregation.addStage(new UnsetStage(unset));
                    break;
                case "$indexStats":
                    aggregation.addStage(new IndexStatsStage(collection));
                    break;
                case "$out":
                    Object outCollection = stage.get(stageOperation);
                    aggregation.addStage(new OutStage(database, outCollection));
                    break;
                case "$redact":
                    Document redactExpression = (Document) stage.get(stageOperation);
                    aggregation.addStage(new RedactStage(redactExpression));
                    break;
                case "$merge":
                    Object mergeParams = stage.get(stageOperation);
                    aggregation.addStage(new MergeStage(databaseResolver, database, mergeParams));
                    break;
                case "$geoNear":
                    throw new MongoServerNotYetImplementedException(138, stageOperation);
                default:
                    throw new MongoServerError(40324, "Unrecognized pipeline stage name: '" + stageOperation + "'");
            }
        }
        return aggregation;
    }

    private List<Document> runStages() {
        return runStages(collection.queryAllAsStream());
    }

    public List<Document> runStages(Stream<Document> stream) {
        return runStagesAsStream(stream).collect(Collectors.toList());
    }

    public Stream<Document> runStagesAsStream(Stream<Document> stream) {
        if (hasVariables()) {
            stream = stream.map(this::addAllVariables);
        }
        for (AggregationStage stage : stages) {
            stream = stage.apply(stream);
        }
        if (hasVariables()) {
            stream = stream.map(this::removeAllVariables);
        }
        return stream;
    }

    private boolean hasVariables() {
        return !variables.isEmpty();
    }

    private Document addAllVariables(Document document) {
        Document clone = document.clone();
        clone.putAll(variables);
        return clone;
    }

    private Document removeAllVariables(Document document) {
        return CollectionUtils.removeAll(document, variables.keySet());
    }

    private void addStage(AggregationStage stage) {
        this.stages.add(stage);
    }

    public List<Document> computeResult() {
        if (collection == null) {
            return Collections.emptyList();
        }
        try {
            return runStages();
        } catch (TypeMismatchException | FailedToOptimizePipelineError | ProjectStage.InvalidProjectException e) {
            throw e;
        } catch (MongoServerError e) {
            if (e.hasCode(ErrorCode._15998, ErrorCode._40353)) {
                throw e;
            }
            throw new PlanExecutorError(e);
        }
    }

    public void setVariables(Map<String, Object> variables) {
        this.variables = Collections.unmodifiableMap(variables);
    }

    public void validate(Document query) {
        stages.stream()
            .filter(stage -> stage instanceof TerminalStage)
            .map(TerminalStage.class::cast)
            .filter(terminalStage -> !isLastStage(terminalStage))
            .findFirst()
            .ifPresent(nonFinalTerminalStage -> {
                throw new MongoServerError(40601, nonFinalTerminalStage.name() + " can only be the final stage in the pipeline");
            });

        Document cursor = (Document) query.get("cursor");
        if (cursor == null) {
            throw new FailedToParseException("The 'cursor' option is required, except for aggregate with the explain argument");
        } else if (!cursor.isEmpty()) {
            log.warn("Non-empty cursor is not yet implemented. Ignoring.");
        }
    }

    private boolean isLastStage(TerminalStage stage) {
        Assert.notEmpty(stages);
        return stages.indexOf(stage) == stages.size() - 1;
    }

    public boolean isModifying() {
        return stages.stream().anyMatch(AggregationStage::isModifying);
    }

    private static class PlanExecutorError extends MongoServerError {

        private static final long serialVersionUID = 1L;

        private static final String MESSAGE_PREFIX = "PlanExecutor error during aggregation :: caused by :: ";

        private PlanExecutorError(MongoServerError cause) {
            super(cause.getCode(), cause.getCodeName(), MESSAGE_PREFIX + cause.getMessageWithoutErrorCode(),
                cause);
        }
    }
}
