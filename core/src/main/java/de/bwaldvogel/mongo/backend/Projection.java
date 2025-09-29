package de.bwaldvogel.mongo.backend;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.Json;
import de.bwaldvogel.mongo.exception.MongoServerError;

class Projection {

    private final Document fields;
    private final String idField;
    private final boolean onlyExclusions;

    Projection(Document fields, String idField) {
        validateFields(fields);
        this.fields = fields;
        this.idField = idField;
        this.onlyExclusions = onlyExclusions(fields);
    }

    Document projectDocument(Document document) {
        if (document == null) {
            return null;
        }

        Document newDocument = new Document();

        // implicitly add _id if not mentioned
        // http://docs.mongodb.org/manual/tutorial/project-fields-from-query-results/#return-the-specified-fields-and-the-id-field-only
        if (!fields.containsKey(idField)) {
            newDocument.put(idField, document.get(idField));
        }

        if (onlyExclusions) {
            newDocument = document.cloneDeeply();
        }

        for (Entry<String, Object> entry : fields.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            projectField(document, newDocument, key, value);
        }

        return newDocument;
    }

    private static void validateFields(Document fields) {
        for (String key : fields.keySet()) {
            Utils.validateKey(key);
            for (String otherKey : fields.keySet()) {
                if (key.equals(otherKey) || otherKey.length() < key.length()) {
                    continue;
                }
                String pathPrefix = Utils.getShorterPathIfPrefix(key, otherKey);
                if (pathPrefix != null) {
                    List<String> shorterPathFragments = Utils.splitPath(key);
                    List<String> longerPathFragments = Utils.splitPath(otherKey);
                    String remainingPortion = Utils.joinPath(longerPathFragments.subList(shorterPathFragments.size() - 1, longerPathFragments.size()));
                    throw new MongoServerError(31249, "Path collision at " + otherKey + " remaining portion " + remainingPortion);
                }
            }
        }
    }

    private enum Type {
        INCLUSIONS, EXCLUSIONS;

        private static Type fromValue(Object value) {
            if (Utils.isTrue(value)) {
                return INCLUSIONS;
            } else {
                return EXCLUSIONS;
            }
        }
    }

    private boolean onlyExclusions(Document fields) {
        Map<Type, List<String>> nonIdInclusionsAndExclusions = fields.entrySet().stream()
            // Special case: if the idField is to be excluded that's always ok:
            .filter(entry -> !(entry.getKey().equals(idField) && !Utils.isTrue(entry.getValue())))
            .collect(Collectors.groupingBy(
                entry -> Type.fromValue(entry.getValue()),
                Collectors.mapping(Entry::getKey, Collectors.toList())
            ));

        // Mongo will police that all the entries are inclusions or exclusions.
        List<String> inclusions = nonIdInclusionsAndExclusions.getOrDefault(Type.INCLUSIONS, Collections.emptyList());
        List<String> exclusions = nonIdInclusionsAndExclusions.getOrDefault(Type.EXCLUSIONS, Collections.emptyList());

        if (!inclusions.isEmpty() && !exclusions.isEmpty()) {
            List<String> includedFields = nonIdInclusionsAndExclusions.get(Type.INCLUSIONS);
            throw new MongoServerError(31253, "Cannot do inclusion on field " + includedFields.get(0) + " in exclusion projection");
        }
        return inclusions.isEmpty();
    }

    private static void projectField(Document document, Document newDocument, String key, Object projectionValue) {
        if (key.contains(Utils.PATH_DELIMITER)) {
            List<String> pathFragments = Utils.splitPath(key);

            String mainKey = pathFragments.get(0);
            String subKey = Utils.joinTail(pathFragments);

            Object object = document.get(mainKey);
            // do not project the subdocument if it is not of type Document
            if (object instanceof Document) {
                Document newsubDocument = (Document) newDocument.computeIfAbsent(mainKey, k -> new Document());
                projectField((Document) object, newsubDocument, subKey, projectionValue);
            } else if (object instanceof List<?> values) {
                List<Object> newprojectedValues = (List<Object>) newDocument.computeIfAbsent(mainKey, k -> new ArrayList<>());

                if ("$".equals(subKey) && !values.isEmpty()) {
                    Object firstValue = values.get(0);
                    if (firstValue instanceof Document) {
                        newprojectedValues.add(firstValue);
                    }
                } else {
                    if (newprojectedValues.isEmpty()) {
                        // In this case we're projecting in, so start with empty documents:
                        for (Object value : values) {
                            if (value instanceof Document) {
                                newprojectedValues.add(new Document());
                            } // Primitives can never be projected-in.
                        }
                    }

                    // Now loop over the underlying values and project
                    int idx = 0;
                    for (Object value : values) {
                        if (value instanceof Document) {
                            //If this fails it means the newDocument's list differs from the oldDocuments list
                            Document newprojectedDocument = (Document) newprojectedValues.get(idx);

                            projectField((Document) value, newprojectedDocument, subKey, projectionValue);
                            idx++;
                        }
                        // Bit of a kludge here: if we're projecting in then we need to count only the Document instances
                        // but if we're projecting away we need to count everything.
                        else if (!Utils.isTrue(projectionValue)) {
                            idx++;
                        }
                    }
                }
            }
        } else {
            Object value = document.getOrMissing(key);

            if (projectionValue instanceof Document projectionDocument) {
                if (projectionDocument.keySet().equals(Set.of(QueryOperator.ELEM_MATCH.getValue()))) {
                    Document elemMatch = (Document) projectionDocument.get(QueryOperator.ELEM_MATCH.getValue());
                    projectElemMatch(newDocument, elemMatch, key, value);
                } else if (projectionDocument.keySet().equals(Set.of("$slice"))) {
                    Object slice = projectionDocument.get("$slice");
                    projectSlice(newDocument, slice, key, value);
                } else {
                    // ignore
                }
            } else {
                if (Utils.isTrue(projectionValue)) {
                    if (!(value instanceof Missing)) {
                        newDocument.put(key, value);
                    }
                } else {
                    newDocument.remove(key);
                }
            }
        }
    }

    private static void projectElemMatch(Document newDocument, Document elemMatch, String key, Object value) {
        QueryMatcher queryMatcher = new DefaultQueryMatcher();
        if (value instanceof List) {
            ((List<?>) value).stream()
                .filter(sourceObject -> sourceObject instanceof Document)
                .filter(sourceObject -> queryMatcher.matches((Document) sourceObject, elemMatch))
                .findFirst()
                .ifPresent(v -> newDocument.put(key, List.of(v)));
        }
    }

    private static void projectSlice(Document newDocument, Object slice, String key, Object value) {
        if (!(value instanceof List)) {
            newDocument.put(key, value);
            return;
        }
        List<?> values = (List<?>) value;
        int fromIndex = 0;
        int toIndex = values.size();
        if (slice instanceof Integer) {
            int num = ((Integer) slice).intValue();
            if (num < 0) {
                fromIndex = values.size() + num;
            } else {
                toIndex = num;
            }
        } else if (slice instanceof List<?> sliceParams) {
            if (sliceParams.size() != 2) {
                throw new MongoServerError(28724, "First argument to $slice must be an array, but is of type: int");
            }

            if (!(sliceParams.get(0) instanceof Number)) {
                throw new MongoServerError(28724, "First argument to $slice must be an array, but is of type: " + Utils.describeType(sliceParams.get(0)));
            }
            if (!(sliceParams.get(1) instanceof Number)) {
                throw new MongoServerError(28724, "First argument to $slice must be an array, but is of type: int");
            }

            fromIndex = ((Number) sliceParams.get(0)).intValue();
            if (fromIndex < 0) {
                fromIndex += values.size();
            }

            int limit = ((Number) sliceParams.get(1)).intValue();
            if (limit <= 0) {
                throw new MongoServerError(28724, "First argument to $slice must be an array, but is of type: int");
            }
            toIndex = fromIndex + limit;
        } else {
            String sliceJson = Json.toJsonValue(slice);
            throw new MongoServerError(28667, "Invalid $slice syntax. " +
                "The given syntax { $slice: " + sliceJson + " } did not match the find() syntax because :: Location31273: " +
                "$slice only supports numbers and [skip, limit] arrays :: " +
                "The given syntax did not match the expression $slice syntax. :: caused by :: " +
                "Expression $slice takes at least 2 arguments, and at most 3, but 1 were passed in.");
        }
        List<?> slicedValue = values.subList(Math.max(0, fromIndex), Math.min(values.size(), toIndex));
        newDocument.put(key, slicedValue);
    }
}
