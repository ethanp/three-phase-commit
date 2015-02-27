package messages;

/**
 * Ethan Petuchowski 2/27/15
 */
public class PrecommitRequest extends Message {
    public PrecommitRequest(int transactionID) {
        super(Command.PRE_COMMIT, transactionID);
    }
}
