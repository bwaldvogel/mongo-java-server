package de.bwaldvogel.mongo.wire;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

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
import de.bwaldvogel.mongo.util.FutureUtils;
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

    private static final Logger log = LoggerFactory.getLogger(MongoDatabaseHandler.class);

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
        mongoBackend.handleCloseAsync(ctx.channel())
            .thenAcceptAsync(aVoid -> {
                    try {
                        super.channelInactive(ctx);
                    } catch (Exception e) {
                        ctx.fireExceptionCaught(e);
                    }
                },
                ctx.executor());
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ClientRequest object) {
        if (object instanceof MongoQuery) {
            handleQueryAsync((MongoQuery) object).thenAccept(response ->
                ctx.channel().writeAndFlush(response));
        } else if (object instanceof MongoInsert) {
            mongoBackend.handleInsertAsync((MongoInsert) object);
        } else if (object instanceof MongoDelete) {
            mongoBackend.handleDeleteAsync((MongoDelete) object);
        } else if (object instanceof MongoUpdate) {
            mongoBackend.handleUpdateAsync((MongoUpdate) object);
        } else if (object instanceof MongoGetMore) {
            handleGetMoreAsync((MongoGetMore) object).thenAccept(response ->
                ctx.channel().writeAndFlush(response));
        } else if (object instanceof MongoKillCursors) {
            mongoBackend.handleKillCursorsAsync((MongoKillCursors) object);
        } else if (object instanceof MongoMessage) {
            handleMessageAsync((MongoMessage) object).thenAccept(response ->
                ctx.channel().writeAndFlush(response));
        } else {
            throw new MongoServerException("unknown message: " + object);
        }
    }

    // visible for testing
    CompletionStage<MongoMessage> handleMessageAsync(MongoMessage message) {
        CompletionStage<Document> handleMessageAsyncResponse;
        try {
            handleMessageAsyncResponse = mongoBackend.handleMessageAsync(message);
        } catch (Throwable t) {
            handleMessageAsyncResponse = FutureUtils.failedFuture(t);
        }
        return handleMessageAsyncResponse.handle(handleMessageAsyncBiFunc(message));
    }

    private BiFunction<Document, Throwable, MongoMessage> handleMessageAsyncBiFunc(MongoMessage message) {
        return (document, t) -> {
            if (t != null) {
                if (t instanceof CompletionException) {
                    t = t.getCause();
                }

                MongoServerException e;
                if (t instanceof MongoServerException) {
                    e = (MongoServerException) t;
                    if (e.isLogError()) {
                        log.error("failed to handle {}", message.getDocument(), e);
                    }
                } else {
                    log.error("Unknown error!", t);
                    e = new MongoServerException("Unknown error: " + t.getMessage(), t);
                }

                document = errorResponse(e, Collections.emptyMap());
            }
            return new MongoMessage(message.getChannel(), createResponseHeader(message), document);
        };
    }

    // visible for testing
    CompletionStage<MongoReply> handleQueryAsync(MongoQuery query) {
        if (query.getCollectionName().startsWith("$cmd")) {
            CompletionStage<Document> handleCommandAsyncResponse;
            try {
                handleCommandAsyncResponse = handleCommandAsync(query);
            } catch (Throwable t) {
                handleCommandAsyncResponse = FutureUtils.failedFuture(t);
            }
            return handleCommandAsyncResponse.handle(handleCommandAsyncBiFunc(query));
        }

        CompletionStage<QueryResult> handleQueryResponseFut;
        try {
            handleQueryResponseFut = mongoBackend.handleQueryAsync(query);
        } catch (Throwable t) {
            handleQueryResponseFut = FutureUtils.failedFuture(t);
        }
        return handleQueryResponseFut.handle(handleQueryAsyncBiFunc(query));
    }

    private BiFunction<Document, Throwable, MongoReply> handleCommandAsyncBiFunc(MongoQuery query) {
        return (document, t) -> {
            MessageHeader header = createResponseHeader(query);
            if (t != null) {
                return createResponseMongoReplyForQueryFailure(header, query, t);
            }

            return new MongoReply(header,
                document != null ? Collections.singletonList(document) : Collections.emptyList(),
                0);
        };
    }

    private BiFunction<QueryResult, Throwable, MongoReply> handleQueryAsyncBiFunc(MongoQuery query) {
        return (queryResult, t) -> {
            MessageHeader header = createResponseHeader(query);
            if (t != null) {
                return createResponseMongoReplyForQueryFailure(header, query, t);
            }

            return new MongoReply(header,
                queryResult != null ? queryResult.collectDocuments() : Collections.emptyList(),
                queryResult != null ? queryResult.getCursorId() : 0);
            };
    }

    private MongoReply createResponseMongoReplyForQueryFailure(MessageHeader header, MongoQuery query, Throwable t) {
        if (t instanceof CompletionException) {
            t = t.getCause();
        }

        if (t instanceof NoSuchCommandException) {
            log.error("unknown command: {}", query, t);
            Map<String, ?> additionalInfo = Collections.singletonMap("bad cmd", query.getQuery());

            return queryFailure(header, (NoSuchCommandException) t, additionalInfo);
        } else if (t instanceof MongoServerException) {
            if (((MongoServerException) t).isLogError()) {
                log.error("failed to handle query {}", query, t);
            }

            return queryFailure(header, (MongoServerException) t, Collections.emptyMap());
        }

        log.error("Unknown error!", t);
        return queryFailure(header,
            new MongoServerException("Unknown error: " + t.getMessage(), t),
            Collections.emptyMap());
    }

    // visible for testing
    CompletionStage<MongoReply> handleGetMoreAsync(MongoGetMore getMore) {
        CompletionStage<QueryResult> handleGetMoreAsyncResponse;
        try {
            handleGetMoreAsyncResponse = mongoBackend.handleGetMoreAsync(getMore);
        } catch (Throwable t) {
            handleGetMoreAsyncResponse = FutureUtils.failedFuture(t);
        }
        return handleGetMoreAsyncResponse.handle(handleGetMoreAsyncBiFunc(getMore));
    }

    private BiFunction<QueryResult, Throwable, MongoReply> handleGetMoreAsyncBiFunc(MongoGetMore getMore) {
        return (queryResult, t) -> {
            MessageHeader header = createResponseHeader(getMore);
            if (t != null) {
                return createResponseMongoReplyForGetMoreFailure(header, getMore, t);
            }

            return new MongoReply(header,
                queryResult != null ? queryResult.collectDocuments() : Collections.emptyList(),
                queryResult != null ? queryResult.getCursorId() : 0);
        };
    }

    private MongoReply createResponseMongoReplyForGetMoreFailure(MessageHeader header, MongoGetMore getMore, Throwable t) {
        if (t instanceof CompletionException) {
            t = t.getCause();
        }

        if (t instanceof CursorNotFoundException) {
            return new MongoReply(header,
                Collections.emptyList(),
                getMore != null ? getMore.getCursorId() : 0,
                ReplyFlag.CURSOR_NOT_FOUND);
        }

        log.error("Unknown error!", t);
        return queryFailure(header,
            new MongoServerException("Unknown error: " + t.getMessage(), t),
            Collections.emptyMap());
    }

    private MessageHeader createResponseHeader(ClientRequest request) {
        return new MessageHeader(idSequence.incrementAndGet(), request.getHeader().getRequestID());
    }

    private MongoReply queryFailure(MessageHeader header, MongoServerException exception, Map<String, ?> additionalInfo) {
        return new MongoReply(header, errorResponse(exception, additionalInfo), ReplyFlag.QUERY_FAILURE);
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

    // visible for testing
    CompletionStage<Document> handleCommandAsync(MongoQuery query) {
        String collectionName = query.getCollectionName();

        if ("$cmd.sys.inprog".equals(collectionName)) {
            return FutureUtils.wrap(() -> mongoBackend.getCurrentOperations(query))
                .thenApply(currentOperations -> new Document("inprog", currentOperations));

        } else if ("$cmd".equals(collectionName)) {
            String command = query.getQuery().keySet().iterator().next();

            switch (command) {
                case "serverStatus":
                    return FutureUtils.wrap(mongoBackend::getServerStatus);

                case "ping":
                    return FutureUtils.wrap(() -> {
                        Document response = new Document();
                        Utils.markOkay(response);
                        return response;
                    });

                default:
                    Document actualQuery = query.getQuery();
                    if ("$query".equals(command)) {
                        command = ((Document) query.getQuery().get("$query")).keySet().iterator().next();
                        actualQuery = (Document) actualQuery.get("$query");
                    }
                    return mongoBackend.handleCommandAsync(query.getChannel(),
                        query.getDatabaseName(),
                        command,
                        actualQuery);
            }
        }

        return FutureUtils.failedFuture(new MongoServerException("unknown collection: " + collectionName));
    }

}
