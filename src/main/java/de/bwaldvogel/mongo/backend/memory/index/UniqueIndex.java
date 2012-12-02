package de.bwaldvogel.mongo.backend.memory.index;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.bson.BSONObject;

import de.bwaldvogel.mongo.exception.DuplicateKeyException;
import de.bwaldvogel.mongo.exception.KeyConstraintException;

public class UniqueIndex extends Index {

    private final String key;
    private Map<Object, Object> index = new TreeMap<Object, Object>();
    private String idField;

    public UniqueIndex(String name , String key , String idField) {
        super( name );
        this.key = key;
        this.idField = idField;
    }

    private synchronized Object getKeyValue( BSONObject document ){
        Object value = document.get( key );
        if ( value instanceof Number ) {
            value = Double.valueOf( ( (Number) value ).doubleValue() );
        }
        return value;
    }

    @Override
    public synchronized void remove( BSONObject document ){
        Object value = getKeyValue( document );
        index.remove( value );
    }

    @Override
    public synchronized void checkAdd( BSONObject document ) throws KeyConstraintException{
        Object value = getKeyValue( document );
        if ( value == null )
            return;

        if ( index.containsKey( value ) ) {
            throw new DuplicateKeyException( this , value );
        }
    }

    public synchronized void add( BSONObject document ) throws KeyConstraintException{
        checkAdd( document );
        Object value = getKeyValue( document );
        if ( value != null ) {
            index.put( value, document.get( idField ) );
        }
    }

    @Override
    public synchronized boolean canHandle( BSONObject query ){
        return query.containsField( key );
    }

    @Override
    public synchronized Iterable<Object> getKeys( BSONObject query ){
        Object object = index.get( getKeyValue( query ) );
        if ( object == null ) {
            return Collections.emptyList();
        }
        return Collections.singletonList( object );
    }
}
