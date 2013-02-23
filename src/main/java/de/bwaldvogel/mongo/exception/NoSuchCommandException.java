package de.bwaldvogel.mongo.exception;


public class NoSuchCommandException extends MongoServerException {

    private static final long serialVersionUID = 772416798455878545L;

    private String command;

    public NoSuchCommandException(String command) {
        super("no such cmd: " + command);
        this.command = command;
    }

    public String getCommand() {
        return command;
    }

}
