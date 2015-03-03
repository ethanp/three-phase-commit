package messages;

public class DubCoordinatorMessage extends Message {

	public DubCoordinatorMessage() {
		super(Command.DUB_COORDINATOR, -1);
	}
	
	@Override
	protected void writeAsTokens(TokenWriter writer) {
	}

	@Override
	protected void readFromTokens(TokenReader reader) {
	}
}
