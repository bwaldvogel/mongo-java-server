package de.bwaldvogel.mongo.backend;

import java.util.LinkedList;

public class LimitedList<E> extends LinkedList<E> {

    private static final long serialVersionUID = -4265811949513159615L;

    private int limit;

    public LimitedList(int limit) {
        this.limit = limit;
    }

    @Override
    public boolean add(E o) {
        super.add(o);
        while (size() > limit) {
            super.remove();
        }
        return true;
    }
}