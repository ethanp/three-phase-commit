package system;

/**
 * Ethan Petuchowski 2/17/15
 */
public class Message {
    public Message(Command command) {
        this.command = command;
    }

    public enum Command {
        DUB_COORDINATOR,
        VOTE_REQ,
        PRE_COMMIT,
        COMMIT,
        ABORT,
        YES,
        NO,
        ACK
    }
    Command command;
}
