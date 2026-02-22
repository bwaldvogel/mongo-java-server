package de.bwaldvogel.mongo.backend;

import java.util.Objects;

public class DatabaseCommand {
    private final Command command;
    private final String queryValue;

    private DatabaseCommand(String queryCommand) {
        this.command = Command.parseString(queryCommand);
        this.queryValue = queryCommand;
    }

    private DatabaseCommand(Command command) {
        this.command = command;
        this.queryValue = command.getName();
    }

    public static DatabaseCommand of(String queryValue) {
        return new DatabaseCommand(queryValue);
    }

    public static DatabaseCommand of(Command command) {
        return new DatabaseCommand(command);
    }

    public Command getCommand() {
        return command;
    }

    public String getQueryValue() {
        return queryValue;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof DatabaseCommand other)){
            return false;
        }
        return command == other.command;
    }

    @Override
    public int hashCode() {
        return Objects.hash(command);
    }
}
