package messages;

import java.io.Serializable;

/**
 * Ethan Petuchowski 2/17/15
 */
public class Message implements Serializable {
    public Message(Command command, int transactionID) {
        this.command = command;
        this.transactionID = transactionID;
    }

    public Command getCommand() {
        return command;
    }

    public enum Command {
        DUB_COORDINATOR,
        VOTE_REQ,
        PRE_COMMIT,
        COMMIT,
        ABORT,
        YES,
        NO,
        ACK,
        ADD,
        UPDATE,
        DELETE,
        UR_ELECTED
    }
    Command command;

    public int getTransactionID() {
        return transactionID;
    }

    int transactionID;

    public String toLogString() {
        return command.toString()+" "+transactionID;
    }
}
