package de.bwaldvogel.mongo.wire;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.CursorNotFoundException;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.exception.NoSuchCommandException;
import de.bwaldvogel.mongo.wire.message.ClientRequest;
import de.bwaldvogel.mongo.wire.message.MessageHeader;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoGetMore;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoKillCursors;
import de.bwaldvogel.mongo.wire.message.MongoMessage;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoReply;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;
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
        if (object instanceof MongoQuery) {
            MongoQuery mongoQuery = (MongoQuery) object;
            ctx.channel().writeAndFlush(handleQuery(mongoQuery));
        } else if (object instanceof MongoInsert) {
            MongoInsert insert = (MongoInsert) object;
            mongoBackend.handleInsert(insert);
        } else if (object instanceof MongoDelete) {
            MongoDelete delete = (MongoDelete) object;
            mongoBackend.handleDelete(delete);
        } else if (object instanceof MongoUpdate) {
            MongoUpdate update = (MongoUpdate) object;
            mongoBackend.handleUpdate(update);
        } else if (object instanceof MongoGetMore) {
            MongoGetMore getMore = (MongoGetMore) object;
            ctx.channel().writeAndFlush(handleGetMore(getMore));
        } else if (object instanceof MongoKillCursors) {
            handleKillCursors((MongoKillCursors) object);
        } else if (object instanceof MongoMessage) {
            MongoMessage message = (MongoMessage) object;
            ctx.channel().writeAndFlush(handleMessage(message));
        } else {
            throw new MongoServerException("unknown message: " + object);
        }
    }

    private MongoMessage handleMessage(MongoMessage message) {
        Document document;
        try {
            document = mongoBackend.handleMessage(message);
        } catch (MongoServerException e) {
            if (e.isLogError()) {
                log.error("failed to handle {}", message.getDocument(), e);
            }
            document = errorResponse(e, Collections.emptyMap());
        }
        MessageHeader header = createResponseHeader(message);
        return new MongoMessage(message.getChannel(), header, document);
    }

    private MongoReply handleQuery(MongoQuery query) {
        MessageHeader header = createResponseHeader(query);
        try {
            QueryResult queryResult = null;
            List<Document> documents = new ArrayList<>();
            if (query.getCollectionName().startsWith("$cmd")) {
                documents.add(handleCommand(query));
            } else {
                queryResult = mongoBackend.handleQuery(query);
                documents.addAll(queryResult.collectDocuments());
            }
            return new MongoReply(header, documents, queryResult == null ? 0 : queryResult.getCursorId());
        } catch (NoSuchCommandException e) {
            log.error("unknown command: {}", query, e);
            Map<String, ?> additionalInfo = Collections.singletonMap("bad cmd", query.getQuery());
            return queryFailure(header, e, additionalInfo);
        } catch (MongoServerException e) {
            if (e.isLogError()) {
                log.error("failed to handle query {}", query, e);
            }
            return queryFailure(header, e);
        }
    }

    public MongoReply handleGetMore(MongoGetMore getMore) {
        MessageHeader header = new MessageHeader(idSequence.incrementAndGet(), getMore.getHeader().getRequestID());
        List<Document> documents = new ArrayList<>();
        final QueryResult queryResult;
        try {
            queryResult = mongoBackend.handleGetMore(getMore.getCursorId(), getMore.getNumberToReturn());
        } catch (CursorNotFoundException cursorNotFoundException) {
            return new MongoReply(header, documents, getMore.getCursorId(), ReplyFlag.CURSOR_NOT_FOUND);
        }
        for (Document obj : queryResult) {
            documents.add(obj);
        }
        return new MongoReply(header, documents, queryResult.getCursorId());
    }

    public void handleKillCursors(MongoKillCursors mongoKillCursors) {
        mongoBackend.handleKillCursors(mongoKillCursors);
    }

    private MessageHeader createResponseHeader(ClientRequest request) {
        return new MessageHeader(idSequence.incrementAndGet(), request.getHeader().getRequestID());
    }

    private MongoReply queryFailure(MessageHeader header, MongoServerException exception) {
        Map<String, ?> additionalInfo = Collections.emptyMap();
        return queryFailure(header, exception, additionalInfo);
    }

    private MongoReply queryFailure(MessageHeader header, MongoServerException exception, Map<String, ?> additionalInfo) {
        Document obj = errorResponse(exception, additionalInfo);
        return new MongoReply(header, obj, ReplyFlag.QUERY_FAILURE);
    }

    private Document errorResponse(MongoServerException exception, Map<String, ?> additionalInfo) {
        Document obj = new Document();
        obj.put("$err", exception.getMessageWithoutErrorCode());
        obj.put("errmsg", exception.getMessageWithoutErrorCode());
        if (exception instanceof MongoServerError) {
            MongoServerError error = (MongoServerError) exception;
            obj.put("code", error.getCode());
            obj.putIfNotNull("codeName", error.getCodeName());
        }
        obj.putAll(additionalInfo);
        obj.put("ok", Integer.valueOf(0));
        return obj;
    }

    Document handleCommand(MongoQuery query) {
        String collectionName = query.getCollectionName();
        if (collectionName.equals("$cmd.sys.inprog")) {
            Collection<Document> currentOperations = mongoBackend.getCurrentOperations(query);
            return new Document("inprog", currentOperations);
        }

        if (collectionName.equals("$cmd")) {
            String command = query.getQuery().keySet().iterator().next();

            switch (command) {
                case "serverStatus":
                    return mongoBackend.getServerStatus();
                case "ping":
                    Document response = new Document();
                    Utils.markOkay(response);
                    return response;
                default:
                    Document actualQuery = query.getQuery();

                    if (command.equals("$query")) {
                        command = ((Document) query.getQuery().get("$query")).keySet().iterator().next();
                        actualQuery = (Document) actualQuery.get("$query");
                    }

                    return mongoBackend.handleCommand(query.getChannel(), query.getDatabaseName(), command, actualQuery);
            }
        }

        throw new MongoServerException("unknown collection: " + collectionName);
    }


}
