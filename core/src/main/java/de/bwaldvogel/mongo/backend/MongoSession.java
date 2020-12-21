package de.bwaldvogel.mongo.backend;

import java.util.UUID;

import org.h2.mvstore.tx.Transaction;

public class MongoSession {
    public final UUID id;
    private Transaction tx;

    public MongoSession(UUID id, Transaction tx) {
        this.id = id;
        this.tx = tx;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        MongoSession other = (MongoSession) obj;
        return id.equals(other.id);
    }

    public Transaction getTransaction() {
        return tx;
    }

    public void commit() {
        tx.commit();
    }

}
