package de.bwaldvogel.mongo.wire;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.DatabaseCommand;
import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.wire.message.ClientRequest;
import de.bwaldvogel.mongo.wire.message.MessageHeader;
import de.bwaldvogel.mongo.wire.message.MongoMessage;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoReply;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;

public class MongoDatabaseHandler extends SimpleChannelInboundHandler<ClientRequest> {

    private static final Logger log = LoggerFactory.getLogger(MongoWireProtocolHandler.class);

    private final AtomicInteger idSequence = new AtomicInteger();
    private final MongoBackend mongoBackend;

    private final ChannelGroup channelGroup;

    public MongoDatabaseHandler(MongoBackend mongoBackend, ChannelGroup channelGroup) {
        this.channelGroup = channelGroup;
        this.mongoBackend = mongoBackend;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        channelGroup.add(ctx.channel());
        log.info("client {} connected", ctx.channel());
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.info("channel {} closed", ctx.channel());
        channelGroup.remove(ctx.channel());
        mongoBackend.handleClose(ctx.channel());
        super.channelInactive(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ClientRequest object) {
        if (object instanceof MongoQuery mongoQuery) {
            MongoReply mongoReply = handleQuery(mongoQuery);
            ctx.channel().writeAndFlush(mongoReply);
        } else if (object instanceof MongoMessage mongoMessage) {
            MongoMessage response = handleMessage(mongoMessage);
            ctx.channel().writeAndFlush(response);
        } else {
            throw new MongoServerException("unknown message: " + object);
        }
    }

    // visible for testing
    MongoMessage handleMessage(MongoMessage message) {
        Document document = null;
        try {
            document = mongoBackend.handleMessage(message);
        } catch (MongoServerException e) {
            log.error("failed to handle {}", message.getDocument(), e);
            document = errorResponse(e, Collections.emptyMap());
        } catch (RuntimeException ex) {
            log.error("Unknown error!", ex);
            MongoServerException e = new MongoServerException("Unknown error: " + ex.getMessage(), ex);
            document = errorResponse(e, Collections.emptyMap());
        }
        return new MongoMessage(message.getChannel(), createResponseHeader(message), document);
    }

    private MongoReply handleQuery(MongoQuery query) {
        if (query.getCollectionName().startsWith("$cmd")) {
            Document document = handleCommand(query);
            MessageHeader header = createResponseHeader(query);

            return new MongoReply(header,
                document != null ? List.of(document) : Collections.emptyList(),
                0);
        }

        QueryResult queryResult = mongoBackend.handleQuery(query);
        MessageHeader header = createResponseHeader(query);

        return new MongoReply(header,
            queryResult != null ? queryResult.collectDocuments() : Collections.emptyList(),
            queryResult != null ? queryResult.getCursorId() : 0);
    }

    private MessageHeader createResponseHeader(ClientRequest request) {
        return new MessageHeader(idSequence.incrementAndGet(), request.getHeader().getRequestID());
    }

    private Document errorResponse(MongoServerException exception, Map<String, ?> additionalInfo) {
        Document obj = new Document();
        obj.put("$err", exception.getMessageWithoutErrorCode());
        obj.put("errmsg", exception.getMessageWithoutErrorCode());
        if (exception instanceof MongoServerError error) {
            obj.put("code", error.getCode());
            obj.putIfNotNull("codeName", error.getCodeName());
        }
        obj.putAll(additionalInfo);
        obj.put("ok", Integer.valueOf(0));
        return obj;
    }

    // visible for testing
    Document handleCommand(MongoQuery query) {
        String collectionName = query.getCollectionName();

        if ("$cmd.sys.inprog".equals(collectionName)) {
            Collection<Document> currentOperations = mongoBackend.getCurrentOperations(query);
            return new Document("inprog", currentOperations);

        } else if ("$cmd".equals(collectionName)) {
            String command = query.getQuery().keySet().iterator().next();
            DatabaseCommand cmd = DatabaseCommand.of(command);
            switch (cmd.getCommand()) {
                case SERVER_STATUS:
                    return mongoBackend.getServerStatus();

                case PING:
                    Document response = new Document();
                    Utils.markOkay(response);
                    return response;

                default:
                    Document actualQuery = query.getQuery();
                    if ("$query".equals(command)) {
                        command = ((Document) query.getQuery().get("$query")).keySet().iterator().next();
                        cmd = DatabaseCommand.of(command);
                        actualQuery = (Document) actualQuery.get("$query");
                    }
                    return mongoBackend.handleCommand(query.getChannel(), query.getDatabaseName(), cmd, actualQuery);
            }
        }

        throw new MongoServerException("unknown collection: " + collectionName);
    }

}
