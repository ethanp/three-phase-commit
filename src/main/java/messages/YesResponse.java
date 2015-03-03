package messages;

/**
 * Ethan Petuchowski 2/27/15
 */
public class YesResponse extends Message {
    public YesResponse(Message request) {
        super(Command.YES, request != null ? request.getTransactionID() : -1);
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
