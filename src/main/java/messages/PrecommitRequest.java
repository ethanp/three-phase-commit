package messages;

/**
 * Ethan Petuchowski 2/27/15
 */
public class PrecommitRequest extends Message {
    public PrecommitRequest(int transactionID) {
        super(Command.PRE_COMMIT, transactionID);
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
