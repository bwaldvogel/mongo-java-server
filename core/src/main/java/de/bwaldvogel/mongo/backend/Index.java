package de.bwaldvogel.mongo.backend;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.CannotIndexParallelArraysError;
import de.bwaldvogel.mongo.exception.KeyConstraintError;

public abstract class Index<P> {

    private final String name;
    private final List<IndexKey> keys;
    private final boolean sparse;

    protected Index(String name, List<IndexKey> keys, boolean sparse) {
        this.name = name;
        this.keys = keys;
        this.sparse = sparse;
    }

    protected boolean isSparse() {
        return sparse;
    }

    protected List<IndexKey> getKeys() {
        return keys;
    }

    public String getName() {
        return name;
    }

    protected List<String> keys() {
        return keys.stream()
            .map(IndexKey::getKey)
            .collect(Collectors.toList());
    }

    protected Set<String> keySet() {
        return keys.stream()
            .map(IndexKey::getKey)
            .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    Set<KeyValue> getKeyValues(Document document) {
        return getKeyValues(document, true);
    }

    Set<KeyValue> getKeyValues(Document document, boolean normalize) {
        Map<String, Object> valuesPerKey = new LinkedHashMap<>();
        for (String key : keys()) {
            Object value = Utils.getSubdocumentValueCollectionAware(document, key);
            if (normalize) {
                value = Utils.normalizeValue(value);
            }
            valuesPerKey.put(key, value);
        }

        Map<String, Object> collectionValues = valuesPerKey.entrySet().stream()
            .filter(entry -> entry.getValue() instanceof Collection)
            .collect(StreamUtils.toLinkedHashMap());

        if (collectionValues.size() == 1) {
            @SuppressWarnings("unchecked")
            Collection<Object> collectionValue = (Collection<Object>) CollectionUtils.getSingleElement(collectionValues.values());
            return CollectionUtils.multiplyWithOtherElements(valuesPerKey.values(), collectionValue).stream()
                .map(KeyValue::new)
                .collect(StreamUtils.toLinkedHashSet());
        } else if (collectionValues.size() > 1) {
            throw new CannotIndexParallelArraysError(collectionValues.keySet());
        } else {
            return Collections.singleton(new KeyValue(valuesPerKey.values()));
        }
    }

    public abstract P getPosition(Document document);

    public abstract void checkAdd(Document document, MongoCollection<P> collection);

    public abstract void add(Document document, P position, MongoCollection<P> collection);

    public abstract P remove(Document document);

    public abstract boolean canHandle(Document query);

    public abstract Iterable<P> getPositions(Document query);

    public abstract long getCount();

    public boolean isEmpty() {
        return getCount() == 0;
    }

    public abstract long getDataSize();

    public abstract void checkUpdate(Document oldDocument, Document newDocument, MongoCollection<P> collection);

    public abstract void updateInPlace(Document oldDocument, Document newDocument, P position, MongoCollection<P> collection) throws KeyConstraintError;

    protected boolean isCompoundIndex() {
        return keys().size() > 1;
    }

    protected boolean nullAwareEqualsKeys(Document oldDocument, Document newDocument) {
        Set<KeyValue> oldKeyValues = getKeyValues(oldDocument);
        Set<KeyValue> newKeyValues = getKeyValues(newDocument);
        return Utils.nullAwareEquals(oldKeyValues, newKeyValues);
    }

    public abstract void drop();

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[name=" + getName() + "]";
    }

}
