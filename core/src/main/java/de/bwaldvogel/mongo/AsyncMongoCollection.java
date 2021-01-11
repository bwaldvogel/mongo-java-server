package de.bwaldvogel.mongo;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

import de.bwaldvogel.mongo.backend.QueryParameters;
import de.bwaldvogel.mongo.backend.QueryResult;

public interface AsyncMongoCollection {

    CompletionStage<QueryResult> handleQueryAsync(QueryParameters queryData);
}
