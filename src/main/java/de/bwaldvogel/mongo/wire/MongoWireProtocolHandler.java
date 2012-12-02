package de.bwaldvogel.mongo.wire;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.bson.BSONDecoder;
import org.bson.BSONObject;
import org.bson.BasicBSONDecoder;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;

import de.bwaldvogel.mongo.wire.message.MessageHeader;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;

public class MongoWireProtocolHandler extends LengthFieldBasedFrameDecoder {

    private static final Logger log = Logger.getLogger( MongoWireProtocolHandler.class );

    private static final int maxFrameLength = Integer.MAX_VALUE;
    private static final int lengthFieldOffset = 0;
    private static final int lengthFieldLength = 4;

    private static final int FLAG_UPSERT = 1 << 0;
    private static final int FLAG_MULTI_UPDATE = 1 << 1;

    private final BSONDecoder bsonDecoder = new BasicBSONDecoder();

    public MongoWireProtocolHandler() {
        super( maxFrameLength , lengthFieldOffset , lengthFieldLength );
    }

    public void exceptionCaught( ChannelHandlerContext ctx , ExceptionEvent e ) throws Exception{
        log.error( "exception for client " + e.getChannel().getId(), e.getCause() );
        e.getChannel().close();
    }

    @Override
    protected Object decode( ChannelHandlerContext ctx , Channel channel , ChannelBuffer buffer ) throws Exception{

        if ( buffer.readableBytes() < 4 ) {
            return null;
        }

        int totalLength = buffer.readInt();
        int endIndex = buffer.readerIndex() + totalLength - 4;

        if ( buffer.readableBytes() < totalLength - 4 ) {
            return null;
        }

        final int requestID = buffer.readInt();
        final int responseTo = buffer.readInt();
        final MessageHeader header = new MessageHeader( requestID , responseTo );
        final OpCode opCode = OpCode.getById( buffer.readInt() );

        int clientId = channel.getId().intValue();
        Object ret;

        switch ( opCode ) {
            case OP_QUERY:
                ret = handleQuery( clientId, header, buffer, endIndex );
                break;
            case OP_INSERT:
                ret = handleInsert( clientId, header, buffer, endIndex );
                break;
            case OP_DELETE:
                ret = handleDelete( clientId, header, buffer, endIndex );
                break;
            case OP_UPDATE:
                ret = handleUpdate( clientId, header, buffer, endIndex );
                break;
            default:
                throw new UnsupportedOperationException( "unsupported opcode: " + opCode );
        }

        if ( buffer.readerIndex() != endIndex ) {
            throw new IOException();
        }

        return ret;
    }

    private Object handleDelete( int clientId , MessageHeader header , ChannelBuffer buffer , int endIndex ) throws IOException{

        buffer.skipBytes( 4 ); // reserved

        final String fullCollectionName = readCString( buffer );
        log.debug( "delete " + header.getRequestID() + " @ " + fullCollectionName );

        final int flags = buffer.readInt();
        if ( flags != 0 )
            throw new UnsupportedOperationException( "flags=" + flags + " not yet supported" );

        BSONObject selector = readBSON( buffer );
        return new MongoDelete( clientId , header , fullCollectionName , selector );
    }

    private Object handleUpdate( int clientId , MessageHeader header , ChannelBuffer buffer , int endIndex ) throws IOException{

        buffer.skipBytes( 4 ); // reserved

        final String fullCollectionName = readCString( buffer );
        log.debug( "update " + header.getRequestID() + " @ " + fullCollectionName );

        final int flags = buffer.readInt();
        boolean upsert = ( ( flags & FLAG_UPSERT ) == FLAG_UPSERT );
        boolean multi = ( ( flags & FLAG_MULTI_UPDATE ) == FLAG_MULTI_UPDATE );

        BSONObject selector = readBSON( buffer );
        BSONObject update = readBSON( buffer );
        return new MongoUpdate( clientId , header , fullCollectionName , selector , update , upsert , multi );
    }

    private Object handleInsert( int clientId , MessageHeader header , ChannelBuffer buffer , int endIndex ) throws IOException{

        final int flags = buffer.readInt();
        if ( flags != 0 )
            throw new UnsupportedOperationException( "flags=" + flags + " not yet supported" );

        final String fullCollectionName = readCString( buffer );
        log.debug( "insert " + header.getRequestID() + " @ " + fullCollectionName );

        List<BSONObject> documents = new ArrayList<BSONObject>();
        while ( buffer.readerIndex() < endIndex ) {
            BSONObject document = readBSON( buffer );
            if ( document == null ) {
                return null;
            }
            documents.add( document );
        }
        return new MongoInsert( clientId , header , fullCollectionName , documents );
    }

    private Object handleQuery( int clientId , MessageHeader header , ChannelBuffer buffer , int endIndex ) throws IOException{

        final int flags = buffer.readInt();
        if ( flags != 0 )
            throw new UnsupportedOperationException( "flags=" + flags + " not yet supported" );

        final String fullCollectionName = readCString( buffer );
        log.debug( "query " + header.getRequestID() + " @ " + fullCollectionName );

        final int numberToSkip = buffer.readInt();
        if ( numberToSkip != 0 )
            throw new UnsupportedOperationException();

        final int numberToReturn = buffer.readInt();
        if ( numberToReturn != -1 && numberToReturn != 0 )
            throw new UnsupportedOperationException();

        BSONObject query = readBSON( buffer );
        BSONObject returnFieldSelector = null;
        if ( buffer.readerIndex() < endIndex ) {
            returnFieldSelector = readBSON( buffer );
        }
        return new MongoQuery( clientId , header , fullCollectionName , query , returnFieldSelector );
    }

    private BSONObject readBSON( ChannelBuffer buffer ) throws IOException{
        // TODO read BSON using Netty
        final int length = buffer.getInt( buffer.readerIndex() );
        final InputStream inputStream = new ByteArrayInputStream( buffer.array() , buffer.readerIndex() , length );
        final BSONObject object = bsonDecoder.readObject( inputStream );
        buffer.skipBytes( length );
        return object;
    }

    private String readCString( ChannelBuffer buffer ){
        final StringBuilder sb = new StringBuilder( 32 );
        while ( true ) {
            final char b = (char) buffer.readByte();
            if ( b == 0 ) {
                break;
            }
            sb.append( b );
        }

        return sb.toString();
    }
}
