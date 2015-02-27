package messages;

/**
 * Ethan Petuchowski 2/27/15
 */
public class NoResponse extends Message {
    public NoResponse(Message responseTo) {
        super(Command.NO, responseTo.getMsgId());
    }
}
