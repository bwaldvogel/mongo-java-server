package de.bwaldvogel.mongo.backend.aggregation.stage;

import static de.bwaldvogel.mongo.backend.Constants.ID_FIELD;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.backend.ValueComparator;
import de.bwaldvogel.mongo.backend.aggregation.Expression;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.FailedToParseException;

public class GraphLookupStage extends AbstractLookupStage {

    private static final String START_WITH = "startWith";
    private static final String CONNECT_FROM_FIELD = "connectFromField";
    private static final String CONNECT_TO_FIELD = "connectToField";
    private static final String MAX_DEPTH = "maxDepth";
    private static final String DEPTH_FIELD = "depthField";

    private static final Set<String> CONFIGURATION_KEYS = Set.of(
        FROM, START_WITH, CONNECT_FROM_FIELD, CONNECT_TO_FIELD, AS, MAX_DEPTH, DEPTH_FIELD);

    private final Object startWith;
    private final String connectFromField;
    private final String connectToField;
    private final String asField;
    private final Integer maxDepth;
    private final String depthField;

    private final MongoCollection<?> collection;

    public GraphLookupStage(Document configuration, MongoDatabase mongoDatabase) {
        String from = readStringConfigurationProperty(configuration, FROM);
        collection = mongoDatabase.resolveCollection(from, false);
        if (!configuration.containsKey(START_WITH)) {
            throw new FailedToParseException("missing '" + START_WITH + "' option to $graphLookup stage specification: " + configuration);
        }
        startWith = configuration.get(START_WITH);
        connectFromField = readStringConfigurationProperty(configuration, CONNECT_FROM_FIELD);
        connectToField = readStringConfigurationProperty(configuration, CONNECT_TO_FIELD);
        asField = readStringConfigurationProperty(configuration, AS);
        maxDepth = readOptionalIntegerConfigurationProperty(configuration, MAX_DEPTH);
        depthField = readOptionalStringConfigurationProperty(configuration, DEPTH_FIELD);
        ensureAllConfigurationPropertiesAreKnown(configuration, CONFIGURATION_KEYS);
    }

    @Override
    public String name() {
        return "$graphLookup";
    }

    private Integer readOptionalIntegerConfigurationProperty(Document configuration, String name) {
        Object value = configuration.get(name);
        return switch (value) {
            case null -> null;
            case Integer integer -> integer;
            default -> throw new FailedToParseException("'" + name + "' option to " + name() + " must be an integer, but was type " + Utils.describeType(value));
        };
    }

    private String readOptionalStringConfigurationProperty(Document configuration, String name) {
        Object value = configuration.get(name);
        return switch (value) {
            case null -> null;
            case String string -> string;
            default -> throw new FailedToParseException("'" + name + "' option to " + name() + " must be a string, but was type " + Utils.describeType(value));
        };
    }

    @Override
    public Stream<Document> apply(Stream<Document> stream) {
        return stream.map(this::connectRemoteDocument);
    }

    private Document connectRemoteDocument(Document document) {
        Object value = Expression.evaluateDocument(startWith, document);
        Document result = document.clone();
        result.put(asField, findLinkedDocuments(value));
        return result;
    }

    private List<Document> findLinkedDocuments(Object value) {
        List<Document> linked = new ArrayList<>();
        Deque<SearchValue> pending = new ArrayDeque<>();
        enqueueValues(pending, value, 0);
        Set<Object> visitedIds = new TreeSet<>(ValueComparator.asc());

        while (!pending.isEmpty()) {
            SearchValue current = pending.removeFirst();
            if (maxDepth != null && current.depth() > maxDepth) {
                continue;
            }
            for (Document newDocument : lookupDocuments(current.value())) {
                if (!visitedIds.add(newDocument.get(ID_FIELD))) {
                    continue;
                }
                Object newValue = Utils.getSubdocumentValueCollectionAware(newDocument, connectFromField);
                enqueueValues(pending, newValue, current.depth() + 1);
                Document result = newDocument.clone();
                if (depthField != null) {
                    result.put(depthField, current.depth());
                }
                linked.add(result);
            }
        }
        return linked;
    }

    private List<Document> lookupDocuments(Object value) {
        // Unlike a plain equality query, a null graph link must not match missing fields.
        Object condition = value == null ? new Document("$eq", null).append("$exists", true) : value;
        return collection.handleQueryAsStream(new Document(connectToField, condition)).toList();
    }

    private static void enqueueValues(Deque<SearchValue> pending, Object value, long depth) {
        List<?> values = value instanceof List<?> list ? list : Collections.singletonList(value);
        for (Object item : values) {
            if (!(item instanceof Missing)) {
                pending.addLast(new SearchValue(item, depth));
            }
        }
    }

    private record SearchValue(Object value, long depth) {}

}
