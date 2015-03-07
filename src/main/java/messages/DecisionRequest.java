package messages;

public class DecisionRequest extends Message {

	public DecisionRequest(int transactionId) {
		super(Command.DECISION_REQUEST, transactionId);
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
