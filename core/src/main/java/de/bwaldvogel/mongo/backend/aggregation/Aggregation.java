package de.bwaldvogel.mongo.backend.aggregation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Spliterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.backend.aggregation.stage.AggregationStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.GroupStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.LimitStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.MatchStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.OrderByStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.ProjectStage;
import de.bwaldvogel.mongo.backend.aggregation.stage.SkipStage;
import de.bwaldvogel.mongo.bson.Document;

public class Aggregation {

    private final MongoCollection<?> collection;

    private final List<AggregationStage> stages = new ArrayList<>();

    public Aggregation(MongoCollection<?> collection) {
        this.collection = collection;
    }

    private List<Document> runStages() {
        Spliterator<Document> documents = collection.queryAll().spliterator();
        Stream<Document> stream = StreamSupport.stream(documents, false);
        for (AggregationStage stage : stages) {
            stream = stage.apply(stream);
        }
        return stream.collect(Collectors.toList());
    }

    public void group(Document groupQuery) {
        this.stages.add(new GroupStage(groupQuery));
    }

    public List<Document> getResult() {
        if (collection == null) {
            return Collections.emptyList();
        }
        return runStages();
    }

    public void match(Document query) {
        stages.add(new MatchStage(query));
    }

    public void skip(Number skip) {
        stages.add(new SkipStage(skip.longValue()));
    }

    public void limit(Number limit) {
        stages.add(new LimitStage(limit.longValue()));
    }

    public void project(Document projection) {
        this.stages.add(new ProjectStage(projection));
    }

    public void orderBy(Document orderBy) {
        this.stages.add(new OrderByStage(orderBy));
    }
}
