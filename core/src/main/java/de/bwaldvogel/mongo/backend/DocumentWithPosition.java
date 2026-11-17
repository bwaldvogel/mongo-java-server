package de.bwaldvogel.mongo.backend;

import java.util.Objects;

import de.bwaldvogel.mongo.bson.Document;

public record DocumentWithPosition<P>(Document document, P position) {

    public DocumentWithPosition(Document document, P position) {
        this.document = Objects.requireNonNull(document);
        this.position = Objects.requireNonNull(position);
    }

}
