package messages;

/**
 * Ethan Petuchowski 2/27/15
 */
public class NoResponse extends Message {
    public NoResponse(Message responseTo) {
        super(Command.NO, responseTo.getTransactionID());
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
