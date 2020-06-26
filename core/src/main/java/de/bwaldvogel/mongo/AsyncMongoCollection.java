package de.bwaldvogel.mongo;

import java.util.concurrent.CompletionStage;

import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.bson.Document;

public interface AsyncMongoCollection {

    CompletionStage<QueryResult> handleQueryAsync(Document query, int numberToSkip, int limit, int batchSize, Document returnFieldSelector);
}
