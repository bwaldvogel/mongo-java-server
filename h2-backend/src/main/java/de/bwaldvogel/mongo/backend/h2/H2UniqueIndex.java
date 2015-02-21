package de.bwaldvogel.mongo.backend.h2;

import java.util.Map.Entry;

import org.h2.mvstore.MVMap;

import de.bwaldvogel.mongo.backend.AbstractUniqueIndex;

public class H2UniqueIndex extends AbstractUniqueIndex<Object> {

    private MVMap<Object, Object> mvMap;

    public H2UniqueIndex(String key, boolean ascending, MVMap<Object, Object> mvMap) {
        super(key, ascending);
        this.mvMap = mvMap;
    }

    @Override
    protected Object removeDocument(Object keyValue) {
        return mvMap.remove(keyValue);
    }

    @Override
    protected boolean containsKeyValue(Object keyValue) {
        return mvMap.containsKey(keyValue);
    }

    @Override
    protected void putKeyValue(Object keyValue, Object key) {
        mvMap.put(keyValue, key);
    }

    @Override
    protected Iterable<Entry<Object, Object>> getIterable() {
        return mvMap.entrySet();
    }

    @Override
    protected Object getKey(Object keyValue) {
        return mvMap.get(keyValue);
    }

    @Override
    public long getCount() {
        return mvMap.sizeAsLong();
    }

    @Override
    public long getDataSize() {
        return getCount();
    }

}
