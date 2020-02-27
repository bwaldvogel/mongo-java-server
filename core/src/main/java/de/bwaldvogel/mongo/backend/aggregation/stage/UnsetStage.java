package de.bwaldvogel.mongo.backend.aggregation.stage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;

/**
 * @author Mark Bigler
 */
public class UnsetStage implements AggregationStage {

	private List<String> unsetPaths = new ArrayList<>();

	public UnsetStage(Object input) {
		if (!(input instanceof String) && !(input instanceof Collection<?>)) {
			// FIXME
			throw new MongoServerError(0, "");
		}

		if(input instanceof String) {
			unsetPaths.add((String)input);
		}

		if(input instanceof Collection<?>) {
			for (Object fieldPath : (Collection<?>)input) {
				if(fieldPath instanceof String) {
					unsetPaths.add((String)fieldPath);
				} else {
					// FIXME
					throw new MongoServerError(0, "");
				}
			}
		}
	}

	@Override
	public Stream<Document> apply(Stream<Document> stream) {
		return stream.map(this::unsetDocumentFields);
	}

	Document unsetDocumentFields(Document document) {
		Document result = document.cloneDeeply();
		for (String unsetPath : unsetPaths) {
			Utils.removeSubdocumentValue(result, unsetPath);
		}
		return result;
	}

}
