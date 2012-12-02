package de.bwaldvogel.mongo.wire;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BSONTimestamp;
import org.bson.types.ObjectId;
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

    public static final int MAX_BSON_OBJECT_SIZE = 16777216;

    private static final Logger log = Logger.getLogger( MongoWireProtocolHandler.class );

    private static final int maxFrameLength = Integer.MAX_VALUE;
    private static final int lengthFieldOffset = 0;
    private static final int lengthFieldLength = 4;
    private static final int lengthAdjustment = -lengthFieldLength;
    private static final int initialBytesToStrip = 0;

    private static final int FLAG_UPSERT = 1 << 0;
    private static final int FLAG_MULTI_UPDATE = 1 << 1;

    public MongoWireProtocolHandler() {
        super( maxFrameLength , lengthFieldOffset , lengthFieldLength , lengthAdjustment , initialBytesToStrip );
    }

    @Override
    public void exceptionCaught( ChannelHandlerContext ctx , ExceptionEvent e ) throws Exception {
        log.error( "exception for client " + e.getChannel().getId(), e.getCause() );
        e.getChannel().close();
    }

    @Override
    protected Object decode( ChannelHandlerContext ctx , Channel channel , ChannelBuffer buffer ) throws Exception {

        if ( buffer.readableBytes() < 4 ) {
            return null;
        }

        buffer.markReaderIndex();
        int totalLength = buffer.readInt();
        if ( buffer.readableBytes() < totalLength - lengthFieldLength ) {
            buffer.resetReaderIndex();
            return null; // retry
        }
        buffer = buffer.readSlice( totalLength - lengthFieldLength );
        int readable = buffer.readableBytes();
        if ( readable != totalLength - lengthFieldLength ) {
            throw new IllegalStateException();
        }

        final int requestID = buffer.readInt();
        final int responseTo = buffer.readInt();
        final MessageHeader header = new MessageHeader( requestID , responseTo );

        int opCodeId = buffer.readInt();
        final OpCode opCode = OpCode.getById( opCodeId );
        if ( opCode == null ) {
            throw new IOException( "opCode " + opCodeId + " not supported" );
        }

        int clientId = channel.getId().intValue();
        Object ret;

        switch ( opCode ) {
            case OP_QUERY:
                ret = handleQuery( clientId, header, buffer );
                break;
            case OP_INSERT:
                ret = handleInsert( clientId, header, buffer );
                break;
            case OP_DELETE:
                ret = handleDelete( clientId, header, buffer );
                break;
            case OP_UPDATE:
                ret = handleUpdate( clientId, header, buffer );
                break;
            default:
                throw new UnsupportedOperationException( "unsupported opcode: " + opCode );
        }

        if ( buffer.readable() ) {
            throw new IOException();
        }

        return ret;
    }

    private Object handleDelete( int clientId , MessageHeader header , ChannelBuffer buffer ) throws IOException {

        buffer.skipBytes( 4 ); // reserved

        final String fullCollectionName = readCString( buffer );
        log.debug( "delete " + header.getRequestID() + " @ " + fullCollectionName );

        final int flags = buffer.readInt();
        if ( flags != 0 )
            throw new UnsupportedOperationException( "flags=" + flags + " not yet supported" );

        BSONObject selector = readBSON( buffer );
        return new MongoDelete( clientId , header , fullCollectionName , selector );
    }

    private Object handleUpdate( int clientId , MessageHeader header , ChannelBuffer buffer ) throws IOException {

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

    private Object handleInsert( int clientId , MessageHeader header , ChannelBuffer buffer ) throws IOException {

        final int flags = buffer.readInt();
        if ( flags != 0 )
            throw new UnsupportedOperationException( "flags=" + flags + " not yet supported" );

        final String fullCollectionName = readCString( buffer );
        log.debug( "insert " + header.getRequestID() + " @ " + fullCollectionName );

        List<BSONObject> documents = new ArrayList<BSONObject>();
        while ( buffer.readable() ) {
            BSONObject document = readBSON( buffer );
            if ( document == null ) {
                return null;
            }
            documents.add( document );
        }
        return new MongoInsert( clientId , header , fullCollectionName , documents );
    }

    private Object handleQuery( int clientId , MessageHeader header , ChannelBuffer buffer ) throws IOException {

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
        if ( buffer.readable() ) {
            returnFieldSelector = readBSON( buffer );
        }
        return new MongoQuery( clientId , header , fullCollectionName , query , returnFieldSelector );
    }

    private BSONObject readBSON( ChannelBuffer buffer ) throws IOException {
        // TODO read BSON using Netty
        final int length = buffer.readInt() - 4;
        if ( buffer.readableBytes() < length ) {
            throw new IOException();
        }
        if ( length > MAX_BSON_OBJECT_SIZE ) {
            throw new IOException();
        }

        BSONObject object = new BasicBSONObject();
        int start = buffer.readerIndex();
        while ( buffer.readerIndex() - start < length ) {
            byte type = buffer.readByte();
            if ( type == 0x00 ) {
                return object;
            }
            String name = readCString( buffer );
            Object value;
            switch ( type ) {
                case 0x01: // double
                    value = Double.valueOf( buffer.readDouble() );
                    break;
                case 0x02: // utf-8 string
                    value = readString( buffer );
                    break;
                case 0x03: // embedded document
                    value = readBSON( buffer );
                    break;
                case 0x04: // array
                    value = readArray( buffer );
                    break;
                case 0x05: // data
                    value = readBinary( buffer );
                    break;
                case 0x07: // object id
                    value = readObjectId( buffer );
                    break;
                case 0x08: // boolean
                    switch ( buffer.readByte() ) {
                        case 0x00:
                            value = Boolean.FALSE;
                            break;
                        case 0x01:
                            value = Boolean.TRUE;
                            break;
                        default:
                            throw new IOException( "illegal boolean value" );
                    }
                    break;
                case 0x09: // UTC datetime
                    value = new Date( buffer.readLong() );
                    break;
                case 0x0A: // null
                    value = null;
                    break;
                case 0x10: // int32
                    value = Integer.valueOf( buffer.readInt() );
                    break;
                case 0x11: // Timestamp
                    value = new BSONTimestamp( buffer.readInt() , buffer.readInt() );
                    break;
                case 0x12: // int64
                    value = Long.valueOf( buffer.readLong() );
                    break;
                default:
                    throw new IOException( "unknown type: " + type );
            }
            object.put( name, value );
        }
        throw new IOException( "illegal BSON object" );
    }

    private List<Object> readArray( ChannelBuffer buffer ) throws IOException {
        List<Object> array = new ArrayList<Object>();
        BSONObject arrayObject = readBSON( buffer );
        for ( String key : arrayObject.keySet() ) {
            array.add( arrayObject.get( key ) );
        }
        return array;
    }

    private ObjectId readObjectId( ChannelBuffer buffer ) {
        byte[] b = new byte[12];
        buffer.readBytes( b );
        return new ObjectId( b );
    }

    private String readString( ChannelBuffer buffer ) throws IOException {
        int length = buffer.readInt();
        byte[] data = new byte[length - 1];
        buffer.readBytes( data );
        String s = new String( data , "UTF-8" );
        byte trail = buffer.readByte();
        if ( trail != 0x00 ) {
            throw new IOException();
        }
        return s;
    }

    private String readCString( ChannelBuffer buffer ) {
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

    private Object readBinary( ChannelBuffer buffer ) throws IOException {
        int length = buffer.readInt();
        int subtype = buffer.readByte();
        switch ( subtype ) {
            case 0x00:
            case 0x80: {
                byte[] data = new byte[length];
                buffer.readBytes( data );
                return data;
            }
            case 0x03:
            case 0x04:
                if ( length != 128 / 8 ) {
                    throw new IOException();
                }
                return new UUID( buffer.readLong() , buffer.readLong() );
            default:
                throw new IOException();
        }
    }
}
