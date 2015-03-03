package node.base;

import node.PeerReference;
import system.network.Connection;

import java.util.Collection;

/**
 * Ethan Petuchowski 2/26/15
 */
public abstract class StateMachine {
    private Collection<PeerReference> peerSet = null;

    public Collection<PeerReference> getPeerSet() {
        return peerSet;
    }

    public void setPeerSet(Collection<PeerReference> peerSet) {
        this.peerSet = peerSet;
    }

    public abstract boolean receiveMessage(Connection overConnection);
}
