package de.bwaldvogel.mongo.backend;

public abstract class AbstractCursor implements Cursor {

    protected final long id;

    protected AbstractCursor(long id) {
        this.id = id;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(id: " + id + ")";
    }

}
