package de.bwaldvogel.mongo.backend.memory.index;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.bson.BSONObject;

import de.bwaldvogel.mongo.exception.DuplicateKeyError;
import de.bwaldvogel.mongo.exception.KeyConstraintError;

public class UniqueIndex extends Index {

    private final String key;
    private Map<Object, Integer> index = new HashMap<Object, Integer>();

    public UniqueIndex(String key) {
        super( determineName( key, true ) );
        this.key = key;
    }

    private static String determineName( String key , boolean ascending ) {
        if ( key.equals( "_id" ) ) {
            return "_id_";
        }
        else {
            return key + "_" + ( ascending ? "1" : "-1" );
        }
    }

    private synchronized Object getKeyValue( BSONObject document ) {
        Object value = document.get( key );

        if ( value instanceof Number ) {
            return Double.valueOf( ( (Number) value ).doubleValue() );
        }
        else {
            return value;
        }
    }

    @Override
    public synchronized Integer remove( BSONObject document ) {
        Object value = getKeyValue( document );
        return index.remove( value );
    }

    @Override
    public synchronized void checkAdd( BSONObject document ) throws KeyConstraintError {
        Object value = getKeyValue( document );
        if ( value == null )
            return;

        if ( index.containsKey( value ) ) {
            throw new DuplicateKeyError( this , value );
        }
    }

    @Override
    public synchronized void add( BSONObject document , Integer position ) throws KeyConstraintError {
        checkAdd( document );
        Object value = getKeyValue( document );
        if ( value != null ) {
            index.put( value, position );
        }
    }

    @Override
    public synchronized boolean canHandle( BSONObject query ) {
        return query.containsField( key );
    }

    @Override
    public synchronized Iterable<Integer> getPositions( BSONObject query ) {
        Integer object = index.get( getKeyValue( query ) );
        if ( object == null ) {
            return Collections.emptyList();
        }
        return Collections.singletonList( object );
    }

    @Override
    public long getCount() {
        return index.size();
    }

    @Override
    public long getDataSize() {
        return getCount(); // TODO
    }
}
