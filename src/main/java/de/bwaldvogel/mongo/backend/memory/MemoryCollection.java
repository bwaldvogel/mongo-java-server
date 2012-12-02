package de.bwaldvogel.mongo.backend.memory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.bson.BSONObject;

import com.mongodb.DefaultDBEncoder;

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

    private AtomicLong dataSize = new AtomicLong();

    private List<BSONObject> documents = new ArrayList<BSONObject>();
    private String databaseName;

    public MemoryCollection(String databaseName , String collectionName , String idField) {
        this.databaseName = databaseName;
        if ( collectionName.startsWith( "$" ) )
            throw new IllegalArgumentException( "illegal collection name: " + collectionName );
        this.collectionName = collectionName;
        indexes.add( new UniqueIndex( getFullName() , idField ) );
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

    private Iterable<Integer> matchDocuments( BSONObject query , Iterable<Integer> positions ) {
        List<Integer> answer = new ArrayList<Integer>();
        for ( Integer pos : positions ) {
            BSONObject document = documents.get( pos.intValue() );
            if ( matches( query, document ) ) {
                answer.add( pos );
            }
        }
        return answer;
    }

    private Iterable<Integer> matchDocuments( BSONObject query ) {
        List<Integer> answer = new ArrayList<Integer>();
        for ( int i = 0; i < documents.size(); i++ ) {
            BSONObject document = documents.get( i );
            if ( matches( query, document ) ) {
                answer.add( Integer.valueOf( i ) );
            }
        }
        return answer;
    }

    private Iterable<Integer> matchKeys( BSONObject query ) {
        synchronized ( indexes ) {
            for ( Index index : indexes ) {
                if ( index.canHandle( query ) ) {
                    return matchDocuments( query, index.getPositions( query ) );
                }
            }
        }

        return matchDocuments( query );
    }

    void addDocument( BSONObject document ) throws KeyConstraintError {

        Integer pos = Integer.valueOf( documents.size() );

        for ( Index index : indexes ) {
            index.checkAdd( document );
        }
        for ( Index index : indexes ) {
            index.add( document, pos );
        }
        dataSize.addAndGet( calculateSize( document ) );
        documents.add( document );
    }

    private static long calculateSize( BSONObject document ) {
        return new DefaultDBEncoder().encode( document ).length;
    }

    void removeDocument( BSONObject document ) {
        Integer pos = null;
        for ( Index index : indexes ) {
            pos = index.remove( document );
        }
        if ( pos == null ) {
            // not found
            return;
        }
        dataSize.addAndGet( -calculateSize( document ) );
        documents.remove( pos.intValue() );
    }

    public synchronized int getCount() {
        return documents.size();
    }

    public synchronized Iterable<BSONObject> handleQuery( BSONObject obj ) {

        BSONObject query;
        BSONObject orderby = null;

        if ( obj.containsField( "query" ) ) {
            query = (BSONObject) obj.get( "query" );
            orderby = (BSONObject) obj.get( "orderby" );
        }
        else if ( obj.containsField( "$query" ) ) {
            throw new UnsupportedOperationException();
        }
        else {
            query = obj;
        }

        if ( documents.isEmpty() ) {
            return Collections.emptyList();
        }

        Iterable<Integer> keys = matchKeys( query );

        if ( orderby == null || orderby.keySet().isEmpty() ) {

        }
        else if ( orderby.keySet().size() == 1 ) {
            if ( orderby.keySet().iterator().next().equals( "$natural" ) ) {
                if ( ( (Number) orderby.get( "$natural" ) ).intValue() == -1 ) {
                    throw new UnsupportedOperationException();
                }
            }
            else {
                throw new UnsupportedOperationException();
            }
        }
        else if ( orderby.keySet().size() > 1 ) {
            throw new UnsupportedOperationException();
        }

        List<BSONObject> objs = new ArrayList<BSONObject>();
        for ( Integer pos : keys ) {
            objs.add( documents.get( pos.intValue() ) );
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
        for ( Integer position : matchKeys( update.getSelector() ) ) {
            if ( n > 0 && !update.isMulti() ) {
                throw new MongoServerError( 0 , "no multi flag" );
            }

            n++;

            BSONObject document = documents.get( position.intValue() );
            for ( String key : newDocument.keySet() ) {
                if ( key.contains( "$" ) ) {
                    if ( key.equals( "$set" ) ) {
                        document.putAll( (BSONObject) newDocument.get( "$set" ) );
                        continue;
                    }
                    else {
                        throw new IllegalArgumentException( key );
                    }
                }
                else {
                    document.put( key, newDocument.get( key ) );
                }
            }
        }

        // insert?
        if ( n == 0 && update.isUpsert() ) {
            // TODO: check keys for $
            addDocument( newDocument );
        }
    }

    public int getNumIndexes() {
        return indexes.size();
    }

    public long getDataSize() {
        return dataSize.get();
    }

    public long getIndexSize() {
        long indexSize = 0;
        for ( Index index : indexes ) {
            // actually the data size is expected. we return the count instead
            indexSize += index.getCount();
        }
        return indexSize;
    }

    public int count( BSONObject query ) {
        if ( query.keySet().isEmpty() ) {
            return getCount();
        }

        int count = 0;
        Iterator<Integer> it = matchKeys( query ).iterator();
        while ( it.hasNext() ) {
            it.next();
            count++;
        }
        return count;
    }
}
