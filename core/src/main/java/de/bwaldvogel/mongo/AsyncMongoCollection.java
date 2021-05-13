package de.bwaldvogel.mongo;

import java.util.List;
import java.util.concurrent.CompletionStage;

import de.bwaldvogel.mongo.backend.QueryParameters;
import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.bson.Document;

public interface AsyncMongoCollection {

    CompletionStage<QueryResult> handleQueryAsync(QueryParameters queryData);

    CompletionStage<List<Document>> insertDocumentsAsync(List<Document> documents, boolean isOrdered);
}
