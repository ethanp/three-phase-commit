package system;

import messages.DubCoordinatorMessage;
import messages.Message;
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
    private ManagerNodeRef coordinator;
    protected int currentTxnID;

    public TransactionManager(int numNodes) {
        nodes = createNodes(numNodes);
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

    public void broadcast(Message message) {
        getNodes().forEach(n -> n.sendMessage(message));
    }

    public void processRequest(VoteRequest voteRequest) {
        dubCoordinator(1);
        getCoordinator().sendMessage(voteRequest);
    }

    public void sendCoordinator(Message message) {
        getCoordinator().sendMessage(message);
    }

    public void send(int nodeID, Message message) {
        getNodeByID(nodeID).sendMessage(message);
    }

    public void dubCoordinator(int nodeID) {
        final ManagerNodeRef newCoord = remoteNodeWithID(nodeID);
        newCoord.sendMessage(new DubCoordinatorMessage());
        setCoordinator(newCoord);
    }

    public ManagerNodeRef getNodeByID(int nodeID) {
        return getNodes().stream().filter(n -> n.getNodeID() == nodeID).findFirst().get();
    }

    public ManagerNodeRef getCoordinator() {
        return coordinator;
    }

    public void setCoordinator(ManagerNodeRef coordinator) {
        this.coordinator = coordinator;
    }
}
