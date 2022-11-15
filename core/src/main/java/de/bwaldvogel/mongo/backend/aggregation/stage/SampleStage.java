package de.bwaldvogel.mongo.backend.aggregation.stage;

import java.security.SecureRandom;
import java.util.Comparator;
import java.util.stream.Stream;

import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class SampleStage implements AggregationStage {

    private static final SecureRandom random = new SecureRandom();

    private final int size;

    public SampleStage(Object sample) {
        if (!(sample instanceof Document)) {
            throw new MongoServerError(28745, "the $sample stage specification must be an object");
        }

        Document size = (Document) sample;
        Object sizeValue = size.getOrMissing("size");

        if (sizeValue instanceof Missing) {
            throw new MongoServerError(28749, "$sample stage must specify a size");
        }

        if (!(sizeValue instanceof Number)) {
            throw new MongoServerError(28746, "size argument to $sample must be a number");
        }

        this.size = ((Number) sizeValue).intValue();

        if (this.size < 0) {
            throw new MongoServerError(28747, "size argument to $sample must not be negative");
        }

        for (String key : size.keySet()) {
            if (!key.equals("size")) {
                throw new MongoServerError(28748, "unrecognized option to $sample: " + key);
            }
        }
    }

    @Override
    public Stream<Document> apply(Stream<Document> stream) {
        return stream
            .sorted(Comparator.comparingDouble(key -> random.nextDouble()))
            .limit(size);
    }
}
