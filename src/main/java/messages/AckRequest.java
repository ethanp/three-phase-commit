package messages;

public class AckRequest extends Message {
    public AckRequest(int transactionID) {
        super(Command.ACK, transactionID);
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
