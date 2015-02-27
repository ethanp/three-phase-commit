package messages;

/**
 * Ethan Petuchowski 2/27/15
 */
public class CommitRequest extends Message {
    public CommitRequest(int transactionID) {
        super(Command.COMMIT, transactionID);
    }
}
