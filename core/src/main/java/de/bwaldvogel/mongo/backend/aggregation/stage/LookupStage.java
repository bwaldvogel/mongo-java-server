package de.bwaldvogel.mongo.backend.aggregation.stage;

import static java.util.stream.Collectors.toList;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.bson.Document;

public class LookupStage extends AbstractLookupStage {

    private static final String LOCAL_FIELD = "localField";
    private static final String FOREIGN_FIELD = "foreignField";

    private static final Set<String> CONFIGURATION_KEYS;
    static {
        CONFIGURATION_KEYS = new HashSet<>();
        CONFIGURATION_KEYS.add(FROM);
        CONFIGURATION_KEYS.add(LOCAL_FIELD);
        CONFIGURATION_KEYS.add(FOREIGN_FIELD);
        CONFIGURATION_KEYS.add(AS);
    }

    private final String localField;
    private final String foreignField;
    private final String as;

    private final MongoCollection<?> collection;

    public LookupStage(Document configuration, MongoDatabase mongoDatabase) {
        String from = readStringConfigurationProperty(configuration, FROM);
        collection = mongoDatabase.resolveCollection(from, false);
        localField = readStringConfigurationProperty(configuration, LOCAL_FIELD);
        foreignField = readStringConfigurationProperty(configuration, FOREIGN_FIELD);
        as = readStringConfigurationProperty(configuration, AS);
        ensureAllConfigurationPropertiesAreKnown(configuration, CONFIGURATION_KEYS);
    }

    @Override
    public Stream<Document> apply(Stream<Document> stream) {
        return stream.map(this::resolveRemoteField);
    }

    private Document resolveRemoteField(Document document) {
        Object value = document.get(localField);
        List<Document> documents = lookupValue(value);
        Document result = document.clone();
        result.put(as, documents);
        return result;
    }

    private List<Document> lookupValue(Object value) {
        if (value instanceof List) {
            return ((List<?>) value).stream()
                .flatMap(item -> lookupValue(item).stream())
                .collect(toList());
        }
        Document query = new Document(foreignField, value);
        Iterable<Document> queryResult = collection.handleQuery(query);
        return StreamSupport.stream(queryResult.spliterator(), false)
            .collect(toList());
    }
}
