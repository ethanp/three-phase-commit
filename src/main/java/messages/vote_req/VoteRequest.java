package messages.vote_req;

import messages.Message;
import messages.TokenReader;
import messages.TokenWriter;
import node.PeerReference;

import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;

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

    public Collection<PeerReference> getCloneOfPeerSet() {
        return getPeerSet().stream().map(PeerReference::clone).collect(Collectors.toList());
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

    protected void writePeerSetAsTokens(TokenWriter writer) {
    	writer.writeToken(new Integer(peerSet.size()).toString());
        for (PeerReference peer : peerSet) {
            writer.writeToken(new Integer(peer.getNodeID()).toString());
            writer.writeToken(new Integer(peer.getListeningPort()).toString());
        }
    }

    protected void readPeerSetAsTokens(TokenReader reader) {
    	int size = Integer.parseInt(reader.readToken());
    	ArrayList<PeerReference> peers = new ArrayList<PeerReference>();
    	for (int i = 0; i < size; ++i) {
    		PeerReference peer = new PeerReference(
    				Integer.parseInt(reader.readToken()),
    				Integer.parseInt(reader.readToken()));
    		peers.add(peer);
    	}
    	peerSet = peers;
    }

    public void setPeerSet(Collection<PeerReference> peerSet) {
        this.peerSet = peerSet;
    }

    protected abstract String actionLogString();
}
