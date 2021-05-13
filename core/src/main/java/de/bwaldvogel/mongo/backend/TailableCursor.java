package de.bwaldvogel.mongo.backend;

import de.bwaldvogel.mongo.oplog.OplogPosition;

public interface TailableCursor extends Cursor {

    OplogPosition getPosition();

}
