package de.bwaldvogel.mongo.wire;

public class MessageHeader {

    private final int requestID;
    private final int responseTo;

    public MessageHeader(int requestID , int responseTo) {
        this.requestID = requestID;
        this.responseTo = responseTo;
    }

    public int getRequestID(){
        return requestID;
    }

    public int getResponseTo(){
        return responseTo;
    }

}
