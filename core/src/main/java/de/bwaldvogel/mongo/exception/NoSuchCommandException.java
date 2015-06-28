package de.bwaldvogel.mongo.exception;

public class NoSuchCommandException extends MongoServerError {

    private static final long serialVersionUID = 772416798455878545L;

    private static final int ERROR_CODE = 59;

    private String command;

    public NoSuchCommandException(String command) {
        super(ERROR_CODE, "no such cmd: " + command);
        this.command = command;
    }

    public String getCommand() {
        return command;
    }

}
