package messages;

/**
 * Ethan Petuchowski 2/17/15
 */
public class Message {
    public Message(Command command, int msgId) {
        this.command = command;
        this.msgId = msgId;
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

    public int getMsgId() {
        return msgId;
    }

    int msgId;
}
