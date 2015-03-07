package messages;

public class StateRequest extends Message {

	public StateRequest(int transactionId) {
		super(Command.STATE_REQUEST, transactionId);
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
