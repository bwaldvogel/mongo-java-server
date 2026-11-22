package de.bwaldvogel.mongo.backend;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.ErrorCode;
import de.bwaldvogel.mongo.exception.MongoServerError;

/** A finite cursor over a capped collection's insertion order. */
final class CappedCursor extends AbstractCursor {

    private final AbstractMongoCollection<?> collection;
    private final boolean descending;
    private final Predicate<Document> matches;
    private final UnaryOperator<Document> projection;
    private int skip;
    private long remaining;
    private Long position;
    private boolean exhausted;

    CappedCursor(long id, AbstractMongoCollection<?> collection, boolean descending,
                 Predicate<Document> matches, UnaryOperator<Document> projection, int skip, int limit) {
        super(id);
        this.collection = collection;
        this.descending = descending;
        this.matches = matches;
        this.projection = projection;
        this.skip = skip;
        this.remaining = limit > 0 ? limit : Long.MAX_VALUE;
    }

    @Override
    public boolean isEmpty() {
        synchronized (collection) {
            return exhausted;
        }
    }

    @Override
    public List<Document> takeDocuments(int numberToReturn) {
        synchronized (collection) {
            if (exhausted) {
                return List.of();
            }
            int batchSize = numberToReturn > 0 ? numberToReturn : Integer.MAX_VALUE;
            Batch batch = new Batch(batchSize);
            collection.scanCappedDocuments(position, descending, batch::visit);
            if (batch.expectedPosition != null) {
                throw positionLost();
            }
            exhausted = remaining == 0 || batch.documents.size() < batchSize;
            return batch.documents;
        }
    }

    private MongoServerError positionLost() {
        exhausted = true;
        return new MongoServerError(ErrorCode.CappedPositionLost, "CollectionScan died due to position in capped collection being deleted");
    }

    private final class Batch {
        private final int size;
        private final List<Document> documents = new ArrayList<>();
        private Long expectedPosition = position;

        Batch(int size) {
            this.size = size;
        }

        boolean visit(long sequence, Document document) {
            if (expectedPosition != null) {
                if (expectedPosition.longValue() != sequence) {
                    throw positionLost();
                }
                expectedPosition = null;
                return true;
            }
            if (!matches.test(document)) {
                return true;
            }
            if (skip > 0) {
                skip--;
                return true;
            }
            documents.add(projection.apply(document));
            remaining--;
            position = sequence;
            return remaining > 0 && documents.size() < size;
        }
    }
}
