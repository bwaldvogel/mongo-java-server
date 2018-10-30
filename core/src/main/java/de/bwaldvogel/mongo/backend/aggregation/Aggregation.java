package de.bwaldvogel.mongo.backend.aggregation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
        Object idExpression = groupQuery.get(ID_FIELD);

        Map<String, Supplier<Accumulator>> accumulatorSuppliers = parseAccumulators(groupQuery);

        if (collection == null) {
            return;
        }

        Map<Object, Collection<Accumulator>> accumulatorsPerKey = new LinkedHashMap<>();
        for (Document document : queryDocuments()) {
            Object key = Expression.evaluate(idExpression, document);

            Collection<Accumulator> accumulators = accumulatorsPerKey.computeIfAbsent(key, k -> accumulatorSuppliers.values()
                .stream()
                .map(Supplier::get)
                .collect(Collectors.toList()));

            for (Accumulator accumulator : accumulators) {
                Object expression = accumulator.getExpression();
                accumulator.aggregate(Expression.evaluate(expression, document));
            }
        }

        result = new ArrayList<>();

        for (Entry<Object, Collection<Accumulator>> entry : accumulatorsPerKey.entrySet()) {
            Document groupResult = new Document();
            groupResult.put(ID_FIELD, entry.getKey());

            for (Accumulator accumulator : entry.getValue()) {
                groupResult.put(accumulator.getField(), accumulator.getResult());
            }

            result.add(groupResult);
        }
    }

    private Map<String, Supplier<Accumulator>> parseAccumulators(Document groupStage) throws MongoServerException {
        Map<String, Supplier<Accumulator>> accumulators = new LinkedHashMap<>();
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
                accumulators.put(field, () -> new SumAccumulator(field, expression));
            } else if (groupOperator.equals("$min")) {
                accumulators.put(field, () -> new MinAccumulator(field, expression));
            } else if (groupOperator.equals("$max")) {
                accumulators.put(field, () -> new MaxAccumulator(field, expression));
            } else if (groupOperator.equals("$avg")) {
                accumulators.put(field, () -> new AvgAccumulator(field, expression));
            } else if (groupOperator.equals("$addToSet")) {
                accumulators.put(field, () -> new AddToSetAccumulator(field, expression));
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
