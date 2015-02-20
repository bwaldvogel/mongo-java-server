package de.bwaldvogel.mongo.backend.memory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.bson.BSONObject;

import de.bwaldvogel.mongo.backend.AbstractMongoCollection;
import de.bwaldvogel.mongo.backend.DocumentWithPosition;
import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.backend.Position;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.exception.MongoServerException;

public class MemoryCollection extends AbstractMongoCollection<IntegerPosition> {

    private List<BSONObject> documents = new ArrayList<BSONObject>();
    private Queue<Integer> emptyPositions = new LinkedList<Integer>();

    public MemoryCollection(String databaseName, String collectionName, String idField) {
        super(databaseName, collectionName, idField);
    }

    @Override
    public synchronized void addDocument(BSONObject document) throws MongoServerException {

        Integer pos = emptyPositions.poll();
        if (pos == null) {
            pos = Integer.valueOf(documents.size());
        }

        for (Index<IntegerPosition> index : getIndexes()) {
            index.checkAdd(document);
        }
        for (Index<IntegerPosition> index : getIndexes()) {
            index.add(document, new IntegerPosition(pos.intValue()));
        }
        updateDataSize(Utils.calculateSize(document));
        if (pos.intValue() == documents.size()) {
            documents.add(document);
        } else {
            documents.set(pos.intValue(), document);
        }
    }

    @Override
    protected Iterable<DocumentWithPosition> iterateAllDocuments() {
        return new Iterable<DocumentWithPosition>() {

            @Override
            public Iterator<DocumentWithPosition> iterator() {
                return new Iterator<DocumentWithPosition>() {

                    private int pos = 0;

                    @Override
                    public boolean hasNext() {
                        return pos < documents.size();
                    }

                    @Override
                    public DocumentWithPosition next() {
                        final IntegerPosition position = new IntegerPosition(pos++);
                        final BSONObject document = getDocumentAt(position);
                        return new DocumentWithPosition() {

                            @Override
                            public Position getPosition() {
                                return position;
                            }

                            @Override
                            public BSONObject getDocument() {
                                return document;
                            }
                        };
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    @Override
    public synchronized int count() {
        return documents.size() - emptyPositions.size();
    }

    @Override
    protected Position scanDocumentPosition(BSONObject document) {
        int position = documents.indexOf(document);
        if (position < 0) {
            return null;
        }
        return new IntegerPosition(position);
    }

    @Override
    protected int getRecordCount() {
     return documents.size();
    }

    @Override
    protected int getDeletedCount() {
        return emptyPositions.size();
    }

    @Override
    protected void removeDocumentAt(Position position) {
        int pos = ((IntegerPosition) position).getPosition();
        documents.set(pos, null);
        emptyPositions.add(Integer.valueOf(pos));
    }

    @Override
    protected BSONObject getDocumentAt(Position position) {
        IntegerPosition indexPosition = (IntegerPosition) position;
        return documents.get(indexPosition.getPosition());
    }

}
