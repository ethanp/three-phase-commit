package messages;

import messages.Message.Command;

public class AbortRequest extends Message {
    public AbortRequest(int transactionID) {
        super(Command.ABORT, transactionID);
    }
}
