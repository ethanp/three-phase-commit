package node.base;

import messages.Message;
import node.PeerReference;
import system.network.Connection;

import java.io.EOFException;
import java.util.Collection;

/**
 * Ethan Petuchowski 2/26/15
 */
public abstract class StateMachine {
    private Collection<PeerReference> peerSet = null;
    protected final Node ownerNode;

    protected StateMachine(Node node) {
        ownerNode = node;
    }

    public Collection<PeerReference> getPeerSet() {
        return peerSet;
    }

    public void setPeerSet(Collection<PeerReference> peerSet) {
        this.peerSet = peerSet;
    }

    public abstract boolean receiveMessage(Connection overConnection, Message message);

    public final boolean receiveMessage(Connection connection) {
        try {
            return receiveMessage(connection, connection.receiveMessage());
        }
        catch (EOFException e) {
            e.printStackTrace();
            return false;
        }
    }
}
