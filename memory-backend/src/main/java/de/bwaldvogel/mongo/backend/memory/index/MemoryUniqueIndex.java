package de.bwaldvogel.mongo.backend.memory.index;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import de.bwaldvogel.mongo.backend.AbstractUniqueIndex;

public class MemoryUniqueIndex extends AbstractUniqueIndex<Integer> {

    private Map<Object, Integer> index = new HashMap<Object, Integer>();

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
    protected Integer removeDocument(Object keyValue) {
        return index.remove(keyValue);
    }

    @Override
    protected boolean containsKeyValue(Object keyValue) {
        return index.containsKey(keyValue);
    }

    @Override
    protected void putKeyValue(Object keyValue, Integer key) {
        index.put(keyValue, key);
    }

    @Override
    protected Integer getKey(Object keyValue) {
        return index.get(keyValue);
    }

    @Override
    protected Iterable<Entry<Object, Integer>> getIterable() {
        return index.entrySet();
    }

}
