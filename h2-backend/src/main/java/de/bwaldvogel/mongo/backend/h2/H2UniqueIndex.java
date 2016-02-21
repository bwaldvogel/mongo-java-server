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
    protected Object removeDocument(Object key) {
        return mvMap.remove(NullableKey.of(key));
    }

    @Override
    protected boolean containsKey(Object key) {
        return mvMap.containsKey(NullableKey.of(key));
    }

    @Override
    protected boolean putKeyPosition(Object key, Object position) {
        Object oldValue = mvMap.put(NullableKey.of(key), NullableKey.of(position));
        return oldValue == null;
    }

    @Override
    protected Iterable<Entry<Object, Object>> getIterable() {
        return mvMap.entrySet();
    }

    @Override
    protected Object getPosition(Object key) {
        return mvMap.get(key);
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
