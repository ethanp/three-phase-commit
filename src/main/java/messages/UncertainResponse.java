package messages;

public class UncertainResponse extends Message {

	public UncertainResponse(int transactionID) {
		super(Command.UNCERTAIN, transactionID);
	}
	
	@Override
	protected void writeAsTokens(TokenWriter writer) {
		// TODO Auto-generated method stub

	}

	@Override
	protected void readFromTokens(TokenReader reader) {
		// TODO Auto-generated method stub

	}

}
