package de.bwaldvogel.mongo.backend.aggregation.stage;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.aggregation.Aggregation;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.oplog.Oplog;

public class FacetStage implements AggregationStage {

    private final Map<String, Aggregation> facets = new LinkedHashMap<>();

    public FacetStage(Document facetsConfiguration, MongoDatabase database,
                      MongoCollection<?> collection, Oplog oplog) {
        for (Entry<String, Object> entry : facetsConfiguration.entrySet()) {
            Aggregation aggregation = Aggregation.fromPipeline(entry.getValue(), database, collection, oplog);
            facets.put(entry.getKey(), aggregation);
        }
    }

    @Override
    public Stream<Document> apply(Stream<Document> stream) {
        List<Document> allDocuments = stream.collect(Collectors.toList());
        Document result = new Document();
        for (Entry<String, Aggregation> entry : facets.entrySet()) {
            Aggregation aggregation = entry.getValue();
            List<Document> documents = aggregation.runStages(allDocuments.stream());
            result.put(entry.getKey(), documents);
        }
        return Stream.of(result);
    }

    @Override
    public boolean isModifying() {
        return facets.values().stream().anyMatch(Aggregation::isModifying);
    }
}
