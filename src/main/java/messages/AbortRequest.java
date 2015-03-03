package messages;

import messages.Message.Command;

public class AbortRequest extends Message {
    public AbortRequest(int transactionID) {
        super(Command.ABORT, transactionID);
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
