package messages;

/**
 * Ethan Petuchowski 2/27/15
 */
public class CommitRequest extends Message {
    public CommitRequest(int transactionID) {
        super(Command.COMMIT, transactionID);
    }
    
	@Override
	protected void writeAsTokens(TokenWriter writer) {
		writer.writeToken(new Integer(transactionID).toString());		
	}

	@Override
	protected void readFromTokens(TokenReader reader) {
		transactionID = Integer.parseInt(reader.readToken());		
	}
}
