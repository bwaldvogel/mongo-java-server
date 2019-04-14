package de.bwaldvogel.mongo.backend;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.Json;
import de.bwaldvogel.mongo.exception.BadValueException;
import de.bwaldvogel.mongo.exception.FailedToParseException;

public class ArrayFilters {

    private final Map<String, Object> values;

    private ArrayFilters(Map<String, Object> values) {
        this.values = values;
    }

    static ArrayFilters parse(Document query, Document updateQuery) {
        @SuppressWarnings("unchecked")
        List<Document> arrayFilters = (List<Document>) query.getOrDefault("arrayFilters", Collections.emptyList());
        return parse(arrayFilters, updateQuery);
    }

    private static ArrayFilters parse(List<Document> arrayFilters, Document updateQuery) {
        Map<String, Object> arrayFilterMap = new LinkedHashMap<>();
        for (Document arrayFilter : arrayFilters) {
            if (arrayFilter.isEmpty()) {
                throw new FailedToParseException("Cannot use an expression without a top-level field name in arrayFilters");
            }
            if (arrayFilter.size() > 1) {
                List<String> keys = new ArrayList<>(arrayFilter.keySet());
                throw new FailedToParseException("Error parsing array filter :: caused by ::" +
                    " Expected a single top-level field name, found '" + keys.get(0) + "' and '" + keys.get(1) + "'");
            }
            Entry<String, Object> entry = arrayFilter.entrySet().iterator().next();
            String identifier = entry.getKey();
            Object value = entry.getValue();
            if (arrayFilterMap.put(identifier, value) != null) {
                throw new FailedToParseException("Found multiple array filters with the same top-level field name " + identifier);
            }
        }

        if (!arrayFilterMap.isEmpty()) {
            validate(updateQuery, arrayFilterMap);
        }

        return new ArrayFilters(arrayFilterMap);
    }

    private static void validate(Document updateQuery, Map<String, Object> arrayFilterMap) {
        Set<String> allKeys = updateQuery.values().stream()
            .filter(Document.class::isInstance)
            .map(Document.class::cast)
            .flatMap(d -> d.keySet().stream())
            .collect(Collectors.toSet());

        for (String identifier : arrayFilterMap.keySet()) {
            if (allKeys.stream().noneMatch(key -> key.contains(toOperator(identifier)))) {
                throw new FailedToParseException("The array filter for identifier '" + identifier
                    + "' was not used in the update " + updateQuery.toString(true, "{ ", " }"));
            }
        }
    }

    public static ArrayFilters empty() {
        return new ArrayFilters(Collections.emptyMap());
    }

    public boolean isEmpty() {
        return values.isEmpty();
    }

    @Override
    public String toString() {
        return Json.toJsonValue(new Document(values));
    }

    public boolean contains(String key) {
        return values.keySet().stream()
            .map(ArrayFilters::toOperator)
            .anyMatch(key::contains);
    }

    private Object getArrayFilterQuery(String key) {
        return values.get(fromOperator(key));
    }

    private static String toOperator(String identifier) {
        return "$[" + identifier + "]";
    }

    private static String fromOperator(String key) {
        if (!key.startsWith("$[") || !key.endsWith("]")) {
            throw new IllegalArgumentException("Illegal key: " + key);
        }
        return key.substring("$[".length(), key.length() - "]".length());
    }

    List<String> calculateKeys(Document document, String key) {
        int pos = key.indexOf(".$[");
        if (pos <= 0) {
            throw new BadValueException("Cannot have array filter identifier (i.e. '$[<id>]') element in the first position in path '" + key + "'");
        }
        String path = key.substring(0, pos);
        String subKey = key.substring(pos + 1);

        Object subObject = Utils.getSubdocumentValue(document, path);
        if (subObject instanceof Missing) {
            throw new BadValueException("The path '" + path + "' must exist in the document in order to apply array updates.");
        } else if (!(subObject instanceof List)) {
            throw new BadValueException("Cannot apply array updates to non-array element grades: " + Json.toJsonValue(subObject) + "'");
        }

        Object arrayFilterQuery = getArrayFilterQuery(subKey);
        List<?> values = (List<?>) subObject;
        QueryMatcher queryMatcher = new DefaultQueryMatcher();

        List<String> keys = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            if (queryMatcher.matchesValue(arrayFilterQuery, values.get(i))) {
                keys.add(path + "." + i);
            }
        }

        return keys;
    }
}
