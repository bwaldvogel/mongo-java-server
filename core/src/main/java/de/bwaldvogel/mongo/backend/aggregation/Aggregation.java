package de.bwaldvogel.mongo.backend.aggregation;

import static de.bwaldvogel.mongo.backend.Constants.ID_FIELD;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Spliterator;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class Aggregation {

    private final MongoCollection<?> collection;

    private Document query = new Document();
    private Document projection;
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

    private Stream<Document> queryDocuments() {
        Spliterator<Document> documents = collection.handleQuery(query, skip, limit).spliterator();
        return StreamSupport.stream(documents, false)
            .map(this::projectDocument);
    }

    private Document projectDocument(Document input) {
        return projectDocument(input, projection);
    }

    static Document projectDocument(Document input, Document projection) {
        if (projection == null) {
            return input;
        }
        if (projection.isEmpty()) {
            throw new MongoServerError(40177, "Invalid $project :: caused by :: specification must have at least one field");
        }
        Document result = new Document();

        if (!projection.containsKey(ID_FIELD)) {
            putIfContainsField(input, result, ID_FIELD);
        }

        for (Entry<String, Object> entry : projection.entrySet()) {
            String field = entry.getKey();
            Object projectionValue = entry.getValue();
            if (projectionValue instanceof Number || projectionValue instanceof Boolean) {
                if (Utils.isTrue(projectionValue)) {
                    putIfContainsField(input, result, field);
                }
            } else if (projectionValue == null) {
                result.put(field, null);
            } else {
                Object projectedValue = Expression.evaluate(projectionValue, input);
                if (projectedValue != null) {
                    result.put(field, projectedValue);
                }
            }
        }
        return result;
    }

    private static void putIfContainsField(Document input, Document result, String field) {
        if (input.containsKey(field)) {
            result.put(field, input.get(field));
        }
    }

    public void group(Document groupQuery) {
        if (!groupQuery.containsKey(ID_FIELD)) {
            throw new MongoServerError(15955, "a group specification must include an _id");
        }
        Object idExpression = groupQuery.get(ID_FIELD);

        Map<String, Supplier<Accumulator>> accumulatorSuppliers = parseAccumulators(groupQuery);

        if (collection == null) {
            return;
        }

        Map<Object, Collection<Accumulator>> accumulatorsPerKey = new LinkedHashMap<>();
        queryDocuments().forEach(document -> {
            Object key = Expression.evaluate(idExpression, document);

            Collection<Accumulator> accumulators = accumulatorsPerKey.computeIfAbsent(key, k -> accumulatorSuppliers.values()
                .stream()
                .map(Supplier::get)
                .collect(Collectors.toList()));

            for (Accumulator accumulator : accumulators) {
                Object expression = accumulator.getExpression();
                accumulator.aggregate(Expression.evaluate(expression, document));
            }
        });

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

    private Map<String, Supplier<Accumulator>> parseAccumulators(Document groupStage) {
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

    public List<Document> getResult() {
        if (result == null) {
            if (collection != null) {
                return queryDocuments().collect(Collectors.toList());
            } else {
                return Collections.emptyList();
            }
        }
        return result;
    }

    public void project(Document projection) {
        this.projection = projection;
    }
}
