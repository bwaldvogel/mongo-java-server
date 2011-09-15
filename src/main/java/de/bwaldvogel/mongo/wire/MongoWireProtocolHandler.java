package de.bwaldvogel.mongo.wire;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.log4j.Logger;
import org.bson.BSONObject;
import org.bson.BasicBSONDecoder;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;

import de.bwaldvogel.mongo.MongoQuery;

public class MongoWireProtocolHandler extends LengthFieldBasedFrameDecoder {

    private static final Logger _log = Logger.getLogger( MongoWireProtocolHandler.class );

    private static final int maxFrameLength = Integer.MAX_VALUE;
    private static final int lengthFieldOffset = 0;
    private static final int lengthFieldLength = 4;

    private final BasicBSONDecoder bsonDecoder = new BasicBSONDecoder();

    public MongoWireProtocolHandler() {
        super( maxFrameLength , lengthFieldOffset , lengthFieldLength );
    }

    public void exceptionCaught( ChannelHandlerContext ctx , ExceptionEvent e ) throws Exception{
        _log.error( "uncaught exception", e.getCause() );
    }

    @Override
    protected Object decode( ChannelHandlerContext ctx , Channel channel , ChannelBuffer buffer ) throws Exception{
        buffer.skipBytes( 4 );// skip the length information
        final int requestID = buffer.readInt();
        final int responseTo = buffer.readInt();
        final MessageHeader header = new MessageHeader( requestID , responseTo );
        final OpCode opCode = OpCode.getById( buffer.readInt() );

        switch ( opCode ) {
            case OP_QUERY:
                return handleQuery( header, buffer );
            default:
                throw new UnsupportedOperationException( "unsupported opcode: " + opCode );
        }
    }

    private Object handleQuery( MessageHeader header , ChannelBuffer buffer ) throws IOException{

        final int flags = buffer.readInt();
        if ( flags != 0 )
            throw new UnsupportedOperationException( "flags=" + flags + " not yet supported" );

        final String fullCollectionName = readCString( buffer );
        _log.info( "query " + header.getRequestID() + " @ " + fullCollectionName );

        final int numberToSkip = buffer.readInt();
        if ( numberToSkip != 0 )
            throw new UnsupportedOperationException();

        final int numberToReturn = buffer.readInt();
        if ( numberToReturn != -1 )
            throw new UnsupportedOperationException();

        final BSONObject query = readBSON( buffer );
        BSONObject returnFieldSelector = null;
        if ( buffer.readableBytes() > 0 ) {
            returnFieldSelector = readBSON( buffer );
        }
        return new MongoQuery( header , fullCollectionName , query , returnFieldSelector );
    }

    private BSONObject readBSON( ChannelBuffer buffer ) throws IOException{
        final int length = buffer.getByte( buffer.readerIndex() );
        final InputStream inputStream = new ByteArrayInputStream( buffer.array() , buffer.readerIndex() , length );
        final BSONObject object = bsonDecoder.readObject( inputStream );
        buffer.skipBytes( length );
        return object;
    }

    private String readCString( ChannelBuffer buffer ){
        final StringBuilder sb = new StringBuilder( 32 );
        while ( buffer.readableBytes() > 0 ) {
            final char b = (char) buffer.readByte();
            if ( b == 0 ) {
                break;
            }
            sb.append( b );
        }

        return sb.toString();
    }
}
