package messages.vote_req;

import messages.Message;
import node.PeerReference;

import java.util.Collection;

/**
 * Ethan Petuchowski 2/27/15
 */
public abstract class VoteRequest extends Message {

    private Collection<PeerReference> peerSet;

    public VoteRequest(Command command, int transactionID, Collection<PeerReference> peerSet) {
        super(command, transactionID);
        this.peerSet = peerSet;
    }

    public Collection<PeerReference> getPeerSet() {
        return peerSet;
    }

    public String getPeerSetLogString() {
        StringBuilder sb = new StringBuilder("PEERS "+peerSet.size()+" ");
        for (PeerReference peer : peerSet) {
            sb.append(String.format(
                    "%s %s ",
                    peer.getNodeID(),
                    peer.getListeningPort()));
        }
        return sb.toString();
    }

    @Override public String toLogString() {
        return super.toLogString()+"\n"+actionLogString()+"\n"+
               getPeerSetLogString()+"\n";
    }

    protected abstract String actionLogString();
}
