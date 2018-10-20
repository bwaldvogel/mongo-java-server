package de.bwaldvogel.mongo.backend.aggregation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerException;

public class Aggregation {

    private static final String ID_FIELD = "_id";

    private final MongoCollection<?> collection;
    private Iterable<Document> documents = null;

    public Aggregation(MongoCollection<?> collection) {
        this.collection = collection;
    }

    private Iterable<Document> getDocuments() throws MongoServerException {
        if (documents != null) {
            return documents;
        } else if (collection == null) {
            return Collections.emptyList();
        } else {
            return collection.queryAll();
        }
    }

    public void match(Document query) throws MongoServerException {
        if (documents != null) {
            throw new MongoServerException("Not yet implemented");
        }

        if (collection == null) {
            documents = Collections.emptyList();
        } else {
            documents = collection.handleQuery(query);
        }
    }

    public void skip(Number skip) throws MongoServerException {
        int numSkip = skip.intValue();
        if (getDocuments() instanceof List) {
            List<Document> documents = (List<Document>) getDocuments();
            numSkip = Math.min(documents.size(), numSkip);
            this.documents = documents.subList(numSkip, documents.size());
        } else {
            throw new MongoServerException("Not yet implemented");
        }
    }

    public void limit(Number limit) throws MongoServerException {
        int numLimit = limit.intValue();
        if (documents instanceof List) {
            List<Document> documents = (List<Document>) this.documents;
            numLimit = Math.min(documents.size(), numLimit);
            this.documents = documents.subList(0, numLimit);
        } else {
            throw new MongoServerException("Not yet implemented");
        }
    }

    public void group(Document groupQuery) throws MongoServerException {
        Document groupResult = new Document();
        String id = (String) groupQuery.get(ID_FIELD);
        groupResult.put(ID_FIELD, id);
        if (id != null) {
            throw new MongoServerException("Not yet implemented");
        }

        List<Accumulator> accumulators = parseAccumulators(groupQuery);
        for (Accumulator accumulator : accumulators) {
            accumulator.initialize(groupResult);
        }

        for (Document document : getDocuments()) {
            for (Accumulator accumulator : accumulators) {
                accumulator.aggregate(groupResult, document);
            }
        }

        documents = Collections.singletonList(groupResult);
    }

    private List<Accumulator> parseAccumulators(Document groupStage) throws MongoServerException {
        List<Accumulator> accumulators = new ArrayList<>();
        for (Entry<String, ?> aggregationEntry : groupStage.entrySet()) {
            if (aggregationEntry.getKey().equals(ID_FIELD)) {
                continue;
            }
            String key = aggregationEntry.getKey();
            Document entryValue = (Document) aggregationEntry.getValue();
            if (entryValue.size() != 1) {
                throw new MongoServerException("Not yet implemented");
            }
            Entry<String, Object> aggregation = entryValue.entrySet().iterator().next();
            if (aggregation.getKey().equals("$sum")) {
                if (aggregation.getValue().equals(1)) {
                    accumulators.add(new SimpleSumAccumulation(key));
                } else {
                    throw new MongoServerException("Not yet implemented");
                }
            }
        }
        return accumulators;
    }

    public Iterable<Document> getResult() throws MongoServerException {
        return getDocuments();
    }
}
