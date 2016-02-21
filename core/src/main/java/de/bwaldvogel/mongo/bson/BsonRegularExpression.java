package de.bwaldvogel.mongo.bson;

public class BsonRegularExpression implements Bson {

    private static final long serialVersionUID = 1L;

    private final String pattern;
    private final String options;

    public BsonRegularExpression(String pattern, String options) {
        this.pattern = pattern;
        this.options = options;
    }

    public String getPattern() {
        return pattern;
    }

    public String getOptions() {
        return options;
    }
}
