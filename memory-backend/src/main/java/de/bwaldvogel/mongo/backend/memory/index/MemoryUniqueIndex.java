package de.bwaldvogel.mongo.backend.memory.index;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import de.bwaldvogel.mongo.backend.AbstractUniqueIndex;
import de.bwaldvogel.mongo.backend.NullableKey;

public class MemoryUniqueIndex extends AbstractUniqueIndex<Integer> {

    private Map<Object, Integer> index = new ConcurrentHashMap<>();

    public MemoryUniqueIndex(String key, boolean ascending) {
        super(key, ascending);
    }

    @Override
    public long getCount() {
        return index.size();
    }

    @Override
    public long getDataSize() {
        return getCount(); // TODO
    }

    @Override
    protected Integer removeDocument(Object key) {
        return index.remove(NullableKey.of(key));
    }

    @Override
    protected boolean containsKey(Object key) {
        return index.containsKey(NullableKey.of(key));
    }

    @Override
    protected boolean putKeyPosition(Object key, Integer position) {
        Integer oldValue = index.put(NullableKey.of(key), position);
        return oldValue == null;
    }

    @Override
    protected Integer getPosition(Object key) {
        return index.get(NullableKey.of(key));
    }

    @Override
    protected Iterable<Entry<Object, Integer>> getIterable() {
        return index.entrySet();
    }

}
