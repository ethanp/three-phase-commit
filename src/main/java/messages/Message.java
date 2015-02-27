package messages;

/**
 * Ethan Petuchowski 2/17/15
 */
public class Message {
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
        DELETE
    }
    Command command;

    public int getTransactionID() {
        return transactionID;
    }

    int transactionID;
}
