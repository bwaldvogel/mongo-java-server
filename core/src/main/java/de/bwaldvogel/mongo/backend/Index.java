package de.bwaldvogel.mongo.backend;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.KeyConstraintError;

public abstract class Index<P> {

    private final List<IndexKey> keys;
    private final boolean sparse;

    protected Index(List<IndexKey> keys, boolean sparse) {
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
        if (keys.size() == 1 && CollectionUtils.getSingleElement(keys).getKey().equals(Constants.ID_FIELD)) {
            return Constants.ID_INDEX_NAME;
        }
        return keys.stream()
            .map(indexKey -> indexKey.getKey() + "_" + (indexKey.isAscending() ? "1" : "-1"))
            .collect(Collectors.joining("_"));
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

    KeyValue getKeyValue(Document document) {
        return getKeyValue(document, true);
    }

    KeyValue getKeyValue(Document document, boolean normalize) {
        return new KeyValue(keys().stream()
            .map(key -> Utils.getSubdocumentValue(document, key))
            .map(normalize ? Utils::normalizeValue : Function.identity())
            .collect(Collectors.toList()));
    }

    public abstract void checkAdd(Document document, MongoCollection<P> collection);

    public abstract void add(Document document, P position, MongoCollection<P> collection);

    public abstract P remove(Document document);

    public abstract boolean canHandle(Document query);

    public abstract Iterable<P> getPositions(Document query);

    public abstract long getCount();

    public abstract long getDataSize();

    public abstract void checkUpdate(Document oldDocument, Document newDocument, MongoCollection<P> collection);

    public abstract void updateInPlace(Document oldDocument, Document newDocument, MongoCollection<P> collection) throws KeyConstraintError;

    protected boolean isCompoundIndex() {
        return keys().size() > 1;
    }

    protected boolean nullAwareEqualsKeys(Document oldDocument, Document newDocument) {
        Object oldKey = getKeyValue(oldDocument);
        Object newKey = getKeyValue(newDocument);
        return Utils.nullAwareEquals(oldKey, newKey);
    }
}
