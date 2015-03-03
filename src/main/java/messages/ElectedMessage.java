package messages;

public class ElectedMessage extends Message {
	
	public ElectedMessage(int transactionID) {
		super(Command.UR_ELECTED, transactionID);
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
