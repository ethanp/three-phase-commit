package system.network;

import messages.Message;

import java.io.EOFException;

/**
 * Ethan Petuchowski 2/28/15
 */
public abstract class Connection {

    public Connection(int receiverID) {
        this.receiverID = receiverID;
    }

    protected int receiverID;
    public abstract Message receiveMessage() throws EOFException;
    public abstract void sendMessage(Message o);

    public int getReceiverID() {
        return receiverID;
    }
    public void setReceiverID(int receiverID) {
        this.receiverID = receiverID;
    }

    public abstract boolean isReady();
}
