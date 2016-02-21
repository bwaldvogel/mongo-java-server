package de.bwaldvogel.mongo.backend.memory.index;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import de.bwaldvogel.mongo.backend.AbstractUniqueIndex;

public class MemoryUniqueIndex extends AbstractUniqueIndex<Integer> {

    private Map<Object, Integer> index = new HashMap<>();

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
        return index.remove(key);
    }

    @Override
    protected boolean containsKey(Object key) {
        return index.containsKey(key);
    }

    @Override
    protected boolean putKeyPosition(Object key, Integer position) {
        Integer oldValue = index.put(key, position);
        return oldValue == null;
    }

    @Override
    protected Integer getPosition(Object key) {
        return index.get(key);
    }

    @Override
    protected Iterable<Entry<Object, Integer>> getIterable() {
        return index.entrySet();
    }

}
