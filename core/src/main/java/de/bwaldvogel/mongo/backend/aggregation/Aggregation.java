package de.bwaldvogel.mongo.backend.aggregation;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;
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
        }
        if (collection != null) {
            return collection.queryAll();
        } else {
            return null;
        }
    }

    public void match(Document query) throws MongoServerException {
        if (documents != null) {
            throw new MongoServerException("Not yet implemented");
        }
        if (collection != null) {
            documents = collection.handleQuery(query);
        }
    }

    public void skip(Number skip) throws MongoServerException {
        int numSkip = skip.intValue();
        Iterable<Document> documents = getDocuments();
        if (documents == null) {
            return;
        }
        if (documents instanceof List) {
            List<Document> documentList = (List<Document>) documents;
            numSkip = Math.min(documentList.size(), numSkip);
            this.documents = documentList.subList(numSkip, documentList.size());
        } else {
            throw new MongoServerException("Not yet implemented");
        }
    }

    public void limit(Number limit) throws MongoServerException {
        int numLimit = limit.intValue();
        Iterable<Document> documents = getDocuments();
        if (documents == null) {
            return;
        }
        if (documents instanceof List) {
            List<Document> documentList = (List<Document>) documents;
            numLimit = Math.min(documentList.size(), numLimit);
            this.documents = documentList.subList(0, numLimit);
        } else {
            throw new MongoServerException("Not yet implemented");
        }
    }

    public void group(Document groupQuery) throws MongoServerException {
        if (!groupQuery.containsKey(ID_FIELD)) {
            throw new MongoServerError(15955, "a group specification must include an _id");
        }
        String id = (String) groupQuery.get(ID_FIELD);
        if (id != null) {
            throw new MongoServerException("Not yet implemented");
        }

        Map<String, Accumulator> accumulators = parseAccumulators(groupQuery);

        Iterable<Document> documents = getDocuments();
        if (documents == null) {
            return;
        }
        for (Document document : documents) {
            for (Accumulator accumulator : accumulators.values()) {
                accumulator.aggregate(document);
            }
        }

        Document groupResult = new Document();
        groupResult.put(ID_FIELD, id);
        for (Entry<String, Accumulator> entry : accumulators.entrySet()) {
            groupResult.put(entry.getKey(), entry.getValue().getResult());
        }

        this.documents = Collections.singletonList(groupResult);
    }

    private Map<String, Accumulator> parseAccumulators(Document groupStage) throws MongoServerException {
        Map<String, Accumulator> accumulators = new LinkedHashMap<>();
        for (Entry<String, ?> accumulatorEntry : groupStage.entrySet()) {
            if (accumulatorEntry.getKey().equals(ID_FIELD)) {
                continue;
            }
            String field = accumulatorEntry.getKey();
            Document entryValue = (Document) accumulatorEntry.getValue();
            if (entryValue.size() != 1) {
                throw new MongoServerError(40238, "The field '" + field + "' must specify one accumulator");
            }
            Entry<String, Object> aggregation = entryValue.entrySet().iterator().next();
            String groupOperator = aggregation.getKey();
            Object expression = aggregation.getValue();
            if (groupOperator.equals("$sum")) {
                accumulators.put(field, new SumAccumulator(expression));
            } else if (groupOperator.equals("$min")) {
                accumulators.put(field, new MinAccumulator(expression));
            } else if (groupOperator.equals("$max")) {
                accumulators.put(field, new MaxAccumulator(expression));
            } else {
                throw new MongoServerError(15952, "unknown group operator '" + groupOperator + "'");
            }
        }
        return accumulators;
    }

    public Iterable<Document> getResult() throws MongoServerException {
        Iterable<Document> documents = getDocuments();
        if (documents == null) {
            return Collections.emptyList();
        }
        return documents;
    }
}
