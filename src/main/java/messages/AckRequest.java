package messages;

public class AckRequest extends Message {
    public AckRequest(int transactionID) {
        super(Command.ACK, transactionID);
    }
}
