package de.bwaldvogel.mongo.exception;

import org.bson.BSONObject;
import org.jboss.netty.channel.Channel;

import com.mongodb.BasicDBObject;

public class MongoServerError extends MongoServerException {

    private static final long serialVersionUID = 4998311355923688257L;
    private int errorCode;

    public MongoServerError(int errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public int getErrorCode() {
        return errorCode;
    }

    @Override
    public BSONObject createBSONObject(Channel channel, BSONObject query) {
        BasicDBObject obj = new BasicDBObject("err", getMessage());
        obj.put("code", Integer.valueOf(getErrorCode()));
        obj.put("connectionId", channel.getId());
        obj.put("ok", Integer.valueOf(1));
        return obj;
    }

}
