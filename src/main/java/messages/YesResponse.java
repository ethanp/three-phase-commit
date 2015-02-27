package messages;

/**
 * Ethan Petuchowski 2/27/15
 */
public class YesResponse extends Message {
    public YesResponse(Message request) {
        super(Command.YES, request.getMsgId());
    }
}
