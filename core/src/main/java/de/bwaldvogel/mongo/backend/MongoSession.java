package de.bwaldvogel.mongo.backend;

import java.util.UUID;
import org.h2.mvstore.tx.Transaction;

public class MongoSession implements Cloneable {
    public final UUID id;
    private Transaction tx;

    public MongoSession(UUID id, Transaction tx) {
        this.id = id;
        this.tx = tx;
    }

    public MongoSession() {
        this.id = UUID.randomUUID();
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
        try {
            if (tx != null) {
                tx.commit();
            }
        } catch (Exception ex) {
            tx.rollback();
        }
    }

    public MongoSession clone() {
        return new MongoSession(id, tx);
    }

    public static MongoSession NoopSession() {
        return new MongoSession();
    }


}
