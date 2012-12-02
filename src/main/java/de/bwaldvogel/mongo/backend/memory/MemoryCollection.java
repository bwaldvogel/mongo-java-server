package de.bwaldvogel.mongo.backend.memory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.bson.BSONObject;

import de.bwaldvogel.mongo.backend.memory.index.Index;
import de.bwaldvogel.mongo.backend.memory.index.UniqueIndex;
import de.bwaldvogel.mongo.exception.KeyConstraintError;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;

public class MemoryCollection {

    private static final Logger log = Logger.getLogger( CommonDatabase.class );

    private String collectionName;
    private List<Index> indexes = new ArrayList<Index>();
    private TreeMap<Object, BSONObject> documents = new TreeMap<Object, BSONObject>();
    private String databaseName;

    private String idField;

    public MemoryCollection(String databaseName , String collectionName , String idField) {
        this.idField = idField;
        this.databaseName = databaseName;
        if ( collectionName.startsWith( "$" ) )
            throw new IllegalArgumentException( "illegal collection name: " + collectionName );
        this.collectionName = collectionName;
        indexes.add( new UniqueIndex( getFullName() , idField , idField ) );
    }

    public String getFullName() {
        return databaseName + "." + getCollectionName();
    }

    public String getCollectionName() {
        return collectionName;
    }

    private static boolean matches( BSONObject query , BSONObject object ) {
        for ( String key : query.keySet() ) {
            Object queryValue = query.get( key );
            if ( queryValue == null ) {
                continue;
            }

            if ( !object.containsField( key ) )
                return false;

            Object value = object.get( key );
            if ( value.equals( queryValue ) ) {
                continue;
            }

            if ( value instanceof Number && queryValue instanceof Number ) {
                if ( ( (Number) value ).doubleValue() != ( (Number) queryValue ).doubleValue() ) {
                    return false;
                }
            }
            else {
                return false;
            }
        }

        return true;
    }

    private Iterable<Object> matchDocuments( BSONObject query , Iterable<Object> keys ) {
        List<Object> answer = new ArrayList<Object>();
        for ( Object key : keys ) {
            BSONObject document = documents.get( key );
            if ( matches( query, document ) ) {
                answer.add( key );
            }
        }
        return answer;
    }

    private Iterable<Object> matchKeys( BSONObject query ) {
        synchronized ( indexes ) {
            for ( Index index : indexes ) {
                if ( index.canHandle( query ) ) {
                    return matchDocuments( query, index.getKeys( query ) );
                }
            }
        }

        return matchDocuments( query, documents.keySet() );
    }

    void addDocument( BSONObject document ) throws KeyConstraintError {
        for ( Index index : indexes ) {
            index.checkAdd( document );
        }
        for ( Index index : indexes ) {
            index.add( document );
        }
        documents.put( document.get( idField ), document );
    }

    void removeDocument( BSONObject document ) {
        for ( Index index : indexes ) {
            index.remove( document );
        }
        documents.remove( document.get( idField ) );
    }

    public synchronized int getCount() {
        return documents.size();
    }

    public synchronized Iterable<BSONObject> handleQuery( BSONObject query ) {
        if ( documents.isEmpty() ) {
            return Collections.emptyList();
        }

        List<BSONObject> objs = new ArrayList<BSONObject>();
        for ( Object key : matchKeys( query ) ) {
            objs.add( documents.get( key ) );
        }
        return objs;
    }

    public synchronized void handleInsert( MongoInsert insert ) throws MongoServerError {
        for ( BSONObject document : insert.getDocuments() ) {
            addDocument( document );
            log.debug( "inserted " + document );
        }
    }

    public synchronized void handleDelete( MongoDelete delete ) {
        for ( BSONObject document : handleQuery( delete.getSelector() ) ) {
            removeDocument( document );
        }
    }

    public synchronized void handleUpdate( MongoUpdate update ) throws MongoServerError {
        BSONObject newDocument = update.getUpdate();
        int n = 0;
        for ( BSONObject documentToUpdate : handleQuery( update.getSelector() ) ) {
            if ( n > 0 && !update.isMulti() ) {
                throw new MongoServerError( 0 , "no multi flag" );
            }
            // TODO: allow update operations like '$set'
            removeDocument( documentToUpdate );
            addDocument( newDocument );
            n++;
        }

        if ( n == 0 && update.isUpsert() ) {
            addDocument( newDocument );
        }
    }

    public int getNumIndexes() {
        return indexes.size();
    }
}
