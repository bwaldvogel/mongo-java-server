package de.bwaldvogel.mongo.backend;

import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;

import de.bwaldvogel.mongo.bson.Document;

final class CappedCollectionSupport {

    private CappedCollectionSupport() {
    }

    static <P> void enforceLimits(AbstractMongoCollection<P> collection) {
        if (!collection.isCapped()) {
            return;
        }
        CollectionOptions options = collection.getOptions();
        int count = collection.count();
        // Keep the new document even if it alone exceeds the size limit.
        while (count > 1 && exceedsLimits(options, count, collection.getDataSize())) {
            Document document = collection.removeOldestDocument().document();
            for (Index<P> index : collection.getIndexes()) {
                index.remove(document);
            }
            collection.subtractDocumentSize(document);
            count--;
        }
    }

    static Optional<QueryResult> tryOpenCursor(AbstractMongoCollection<?> collection, Document query,
                                               Document orderBy, int skip, int limit, int batchSize, Document fieldSelector) {
        if (!collection.isCapped() || batchSize <= 0 || AbstractMongoCollection.hasFieldSort(orderBy)) {
            return Optional.empty();
        }

        boolean descending = AbstractMongoCollection.isNaturalDescending(orderBy);
        UnaryOperator<Document> project = UnaryOperator.identity();
        if (fieldSelector != null && !fieldSelector.isEmpty()) {
            Projection projection = new Projection(fieldSelector, collection.getIdField());
            project = projection::projectDocument;
        }
        CursorRegistry registry = collection.cursorRegistry;
        CappedCursor cursor = new CappedCursor(registry.generateCursorId(), collection, descending,
            document -> collection.documentMatchesQuery(document, query), project, skip, limit);
        List<Document> firstBatch = cursor.takeDocuments(batchSize);
        if (cursor.isEmpty()) {
            return Optional.of(new QueryResult(firstBatch));
        }
        registry.add(cursor);
        return Optional.of(new QueryResult(firstBatch, cursor));
    }

    private static boolean exceedsLimits(CollectionOptions options, int count, long dataSize) {
        Long maxSize = options.cappedSize();
        Long maxDocuments = options.cappedMaxDocuments();
        return (maxSize != null && dataSize > maxSize)
            || (maxDocuments != null && maxDocuments > 0 && count > maxDocuments);
    }

    static void addStats(AbstractMongoCollection<?> collection, Document stats) {
        boolean capped = collection.isCapped();
        stats.put("capped", capped);
        if (!capped) {
            return;
        }
        CollectionOptions options = collection.getOptions();
        stats.put("maxSize", options.getCappedSize());
        if (options.getCappedMaxDocuments() != null && options.getCappedMaxDocuments() > 0) {
            stats.put("max", options.getCappedMaxDocuments());
        }
    }
}
