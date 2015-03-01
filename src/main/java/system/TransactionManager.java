package system;

import messages.Message;
import messages.vote_req.AddRequest;
import messages.vote_req.VoteRequest;
import node.PeerReference;
import system.network.Network;
import util.SongTuple;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Ethan Petuchowski 2/17/15
 *
 * This is where program execution begins
 */
public abstract class TransactionManager {

    protected List<ManagerNodeRef> nodes;
    protected Network network;
    protected ManagerNodeRef coordinator;
    protected Failure.Case failureCase;

    protected int currentTxnID;

    public TransactionManager(int numNodes) {
        nodes = createNodes(numNodes);
    }

    public abstract List<ManagerNodeRef> createNodes(int numNodes);

    ManagerNodeRef remoteNodeWithID(int nodeID) {
        for (ManagerNodeRef n : nodes)
            if (n.getID() == nodeID)
                return n;
        return null;
    }

    /**
     * returns true iff the song was successfully committed by all nodes
     */
    public Transaction addSong(SongTuple song) {
        final AddRequest addRequest = new AddRequest(song, ++currentTxnID, nodesToPeerRefs());
        coordinator.sendMessage(addRequest);
        return new Transaction(addRequest) {
            @Override boolean didCommit(boolean result) {
                return result;
            }
        };
    }

    private Collection<PeerReference> nodesToPeerRefs() {
        return nodes.stream().map(n -> new PeerReference(n.nodeID, n.listenPort)).collect(Collectors.toList());
    }

    public List<ManagerNodeRef> getNodes() {
        return nodes;
    }

    public void processRequest(VoteRequest voteRequest) {
        coordinator.sendMessage(voteRequest);
    }

    abstract class Transaction {
        VoteRequest voteRequest;
        public Transaction(VoteRequest request) {
            voteRequest = request;
        }
        abstract boolean didCommit(boolean result);
    }

    public void dubCoordinator(int nodeID) {
        final ManagerNodeRef newCoord = remoteNodeWithID(nodeID);
        newCoord.sendMessage(new Message(Message.Command.DUB_COORDINATOR, -1));
        coordinator = newCoord;
    }
}
