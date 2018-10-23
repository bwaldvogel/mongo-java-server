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

    private Document query = new Document();
    private int skip = 0;
    private int limit = 0;

    private List<Document> result;

    public Aggregation(MongoCollection<?> collection) {
        this.collection = collection;
    }

    public void match(Document query) {
        this.query = query;
    }

    public void skip(Number skip) {
        this.skip = skip.intValue();
    }

    public void limit(Number limit) {
        this.limit = limit.intValue();
    }

    private Iterable<Document> queryDocuments() throws MongoServerException {
        return collection.handleQuery(query, skip, limit);
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

        if (collection == null) {
            return;
        }
        for (Document document : queryDocuments()) {
            for (Accumulator accumulator : accumulators.values()) {
                accumulator.aggregate(document);
            }
        }

        Document groupResult = new Document();
        groupResult.put(ID_FIELD, id);
        for (Entry<String, Accumulator> entry : accumulators.entrySet()) {
            groupResult.put(entry.getKey(), entry.getValue().getResult());
        }

        this.result = Collections.singletonList(groupResult);
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
            } else if (groupOperator.equals("$avg")) {
                accumulators.put(field, new AvgAccumulator(expression));
            } else {
                throw new MongoServerError(15952, "unknown group operator '" + groupOperator + "'");
            }
        }
        return accumulators;
    }

    public Iterable<Document> getResult() throws MongoServerException {
        if (result == null) {
            if (collection != null) {
                return queryDocuments();
            } else {
                return Collections.emptyList();
            }
        }
        return result;
    }
}
