package messages;

import java.util.Collection;

public class InRecoveryResponse extends Message {

	private Collection<Integer> lastUpSet;
	
	public InRecoveryResponse(int transactionID, Collection<Integer> lastUpSet) {
		super(Command.IN_RECOVERY, transactionID);
		this.lastUpSet = lastUpSet;
	}
	
	public Collection<Integer> getLastUpSet() {
		return lastUpSet;
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
