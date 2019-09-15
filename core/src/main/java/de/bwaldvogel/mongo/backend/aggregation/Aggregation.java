package de.bwaldvogel.mongo.backend.aggregation;

import static de.bwaldvogel.mongo.backend.Constants.ID_FIELD;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.CollectionUtils;
import de.bwaldvogel.mongo.backend.aggregation.stage.AddFieldsStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.AggregationStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.BucketStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.FacetStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.GroupStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.LimitStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.LookupStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.LookupWithPipelineStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.MatchStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.OrderByStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.ProjectStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.ReplaceRootStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.SkipStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.UnwindStage;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.TypeMismatchException;

public class Aggregation {

    private final MongoCollection<?> collection;

    private final List<AggregationStage> stages = new ArrayList<>();
    private Map<String, Object> variables = Collections.emptyMap();

    private Aggregation(MongoCollection<?> collection) {
        this.collection = collection;
    }

    public static Aggregation fromPipeline(Object pipelineObject, MongoDatabase database, MongoCollection<?> collection) {
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
        return fromPipeline(pipeline, database, collection);
    }

    private static Aggregation fromPipeline(List<Document> pipeline, MongoDatabase database, MongoCollection<?> collection) {
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
                case "$lookup":
                    Document lookup = (Document) stage.get(stageOperation);
                    if (lookup.containsKey(LookupWithPipelineStage.PIPELINE_FIELD)) {
                        aggregation.addStage(new LookupWithPipelineStage(lookup, database));
                    } else {
                        aggregation.addStage(new LookupStage(lookup, database));
                    }
                    break;
                case "$replaceRoot":
                    Document replaceRoot = (Document) stage.get(stageOperation);
                    aggregation.addStage(new ReplaceRootStage(replaceRoot));
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
                    aggregation.addStage(new FacetStage(facet, database, collection));
                    break;
                default:
                    throw new MongoServerError(40324, "Unrecognized pipeline stage name: '" + stageOperation + "'");
            }
        }
        return aggregation;
    }

    private List<Document> runStages() {
        Spliterator<Document> documents = collection.queryAll().spliterator();
        return runStages(StreamSupport.stream(documents, false));
    }

    public List<Document> runStages(Stream<Document> stream) {
        if (hasVariables()) {
            stream = stream.map(this::addAllVariables);
        }
        for (AggregationStage stage : stages) {
            stream = stage.apply(stream);
        }
        if (hasVariables()) {
            stream = stream.map(this::removeAllVariables);
        }
        return stream.collect(Collectors.toList());
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
        return runStages();
    }

    public void setVariables(Map<String, Object> variables) {
        this.variables = Collections.unmodifiableMap(variables);
    }

}
