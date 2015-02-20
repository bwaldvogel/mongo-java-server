package de.bwaldvogel.mongo.backend;

import org.bson.BSONObject;

public interface DocumentWithPosition {

    BSONObject getDocument();

    Position getPosition();
}
