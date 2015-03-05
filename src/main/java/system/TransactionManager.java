package system;

import console.CommandConsole;
import messages.DubCoordinatorMessage;
import messages.vote_req.VoteRequest;
import node.PeerReference;
import system.network.Network;

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
    protected CommandConsole console;

    protected int currentTxnID;

    public TransactionManager(int numNodes) {
        nodes = createNodes(numNodes);
        console = new CommandConsole(this);
    }

    public abstract List<ManagerNodeRef> createNodes(int numNodes);

    public ManagerNodeRef remoteNodeWithID(int nodeID) {
        for (ManagerNodeRef n : nodes)
            if (n.getNodeID() == nodeID)
                return n;
        return null;
    }

    private Collection<PeerReference> nodesToPeerRefs() {
        return nodes.stream().map(n -> new PeerReference(n.getNodeID(), n.getListenPort())).collect(Collectors.toList());
    }

    public List<ManagerNodeRef> getNodes() {
        return nodes;
    }

    public void processRequest(VoteRequest voteRequest) {
        coordinator.sendMessage(voteRequest);
    }

    public void dubCoordinator(int nodeID) {
        final ManagerNodeRef newCoord = remoteNodeWithID(nodeID);
        newCoord.sendMessage(new DubCoordinatorMessage());
        coordinator = newCoord;
    }
}
