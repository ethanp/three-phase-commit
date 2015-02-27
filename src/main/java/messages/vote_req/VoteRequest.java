package messages.vote_req;

import messages.Message;
import node.PeerReference;

import java.util.Collection;

/**
 * Ethan Petuchowski 2/27/15
 */
public class VoteRequest extends Message {

    private Collection<PeerReference> peerSet;

    public VoteRequest(Command command, int transactionID, Collection<PeerReference> peerSet) {
        super(command, transactionID);
        this.peerSet = peerSet;
    }

    public Collection<PeerReference> getPeerSet() {
        return peerSet;
    }
}
