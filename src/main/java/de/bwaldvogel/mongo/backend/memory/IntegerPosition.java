package de.bwaldvogel.mongo.backend.memory;

import de.bwaldvogel.mongo.backend.Position;

public class IntegerPosition implements Position {

    private final int position;

    public IntegerPosition(int position){
        this.position = position;
    }

    public int getPosition() {
        return position;
    }
}
