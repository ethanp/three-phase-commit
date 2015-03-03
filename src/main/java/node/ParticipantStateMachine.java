package node;

import messages.AbortRequest;
import messages.AckRequest;
import messages.CommitRequest;
import messages.ElectedMessage;
import messages.Message;
import messages.NoResponse;
import messages.PeerTimeout;
import messages.PrecommitRequest;
import messages.YesResponse;
import messages.vote_req.AddRequest;
import messages.vote_req.DeleteRequest;
import messages.vote_req.UpdateRequest;
import messages.vote_req.VoteRequest;
import node.base.Node;
import node.base.StateMachine;
import system.network.Connection;

import java.util.Collection;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import static util.Common.NO_ONGOING_TRANSACTION;

/**
 * Ethan Petuchowski 2/27/15
 */
public class ParticipantStateMachine extends StateMachine {

    /* REFERENCES */
    final Node node;

    /* ONGOING TRANSACTION ATTRIBUTES */
    private int ongoingTransactionID = NO_ONGOING_TRANSACTION;
    private VoteRequest action;  // the update being performed
    private Collection<PeerReference> upSet = null;
    private boolean precommitted = false;
    private Connection currentConnection = null;
    private int coordinatorId;

    public ParticipantStateMachine(Node node) {
        this.node = node;
    }

    @Override public boolean receiveMessage(Connection overConnection) {
        currentConnection = overConnection;

        Message msg = currentConnection.receiveMessage();

        if (msg == null) {
            return false;
        }

        switch (msg.getCommand()) {

            /* VOTE REQUESTS */
            case ADD:
                receiveAddRequest((AddRequest) msg);
                break;
            case UPDATE:
                receiveUpdateRequest((UpdateRequest) msg);
                break;
            case DELETE:
                receiveDeleteRequest((DeleteRequest) msg);
                break;

            /* OTHER */
            case DUB_COORDINATOR:
                receiveDubCoordinator(msg);
                break;
            case PRE_COMMIT:
                receivePrecommit((PrecommitRequest) msg);
                break;
            case COMMIT:
                receiveCommit((CommitRequest) msg);
                break;
            case ABORT:
                receiveAbort(msg);
                break;
            case UR_ELECTED:
                receiveUR_ELECTED(msg);
                break;
            case TIMEOUT:
            	onTimeout((PeerTimeout)msg);
            	break;
            default:
                throw new RuntimeException("Not a valid message: "+msg.getCommand());
        }

        return true;
    }

    private void receiveUR_ELECTED(Message message) {
        node.becomeCoordinator();
    }

    private void receiveAbort(Message message) {
        node.logMessage(message);
        action = null;
        setPeerSet(null);
        setUpSet(null);
        ongoingTransactionID = NO_ONGOING_TRANSACTION;
    }

    private void receiveDubCoordinator(Message message) {
        node.becomeCoordinator();
    }

    private void receiveVoteRequest(VoteRequest voteRequest, boolean voteValue) {
    	setCoordinatorID(currentConnection.getReceiverID());
    	node.logMessage(voteRequest);
        if (voteValue) {
            respondYESToVoteRequest(voteRequest);
        }
        else {
            respondNOToVoteRequest(voteRequest);
        }
    }

    private void receiveAddRequest(AddRequest addRequest) {
        receiveVoteRequest(addRequest, !node.hasSongTupleWithName(addRequest.getSongTuple()));
    }

    private void receiveUpdateRequest(UpdateRequest updateRequest) {
        receiveVoteRequest(updateRequest, node.hasSong(updateRequest.getSongName()));
    }

    private void receiveDeleteRequest(DeleteRequest deleteRequest) {
        receiveVoteRequest(deleteRequest, node.hasSong(deleteRequest.getSongName()));
    }

    /**
     * doesn't log anything (Lecture 3, Pg. 13), send ACK
     */
    private void receivePrecommit(PrecommitRequest precommitRequest) {
        setPrecommitted(true);
        currentConnection.sendMessage(
                new AckRequest(getOngoingTransactionID()));
    }

    private void receiveCommit(CommitRequest commitRequest) {
        node.logMessage(commitRequest);
        node.commitAction(action);
        setOngoingTransactionID(NO_ONGOING_TRANSACTION);
    }

    public void respondNOToVoteRequest(Message message) {
        setOngoingTransactionID(NO_ONGOING_TRANSACTION);
        node.logMessage(new AbortRequest(message.getTransactionID()));
        currentConnection.sendMessage(new NoResponse(message));
    }

    private void respondYESToVoteRequest(VoteRequest voteRequest) {
        action = voteRequest;
        setOngoingTransactionID(voteRequest.getTransactionID());
        setPeerSet(voteRequest.getCloneOfPeerSet());
        setUpSet(voteRequest.getCloneOfPeerSet());
        logAndSendMessage(new YesResponse(voteRequest));
    }

    public void logAndSendMessage(Message message) {
        node.logMessage(message);
        currentConnection.sendMessage(message);
    }

    /* Getters and Setters */

    public int getOngoingTransactionID() {
        return ongoingTransactionID;
    }

    public void setOngoingTransactionID(int ongoingTransactionID) {
        this.ongoingTransactionID = ongoingTransactionID;
    }

    public boolean isPrecommitted() {
        return precommitted;
    }

    public void setPrecommitted(boolean precommitted) {
        this.precommitted = precommitted;
    }

    public void setCoordinatorID(int coordinatorID) {
    	this.coordinatorId = coordinatorID;
    }

    public Collection<PeerReference> getUpSet() {
        return upSet;
    }

    public void setUpSet(Collection<PeerReference> upSet) {
        this.upSet = upSet;
    }

    public VoteRequest getAction() {
        return action;
    }

    public void setAction(VoteRequest action) {
        this.action = action;
    }

    private void onTimeout(PeerTimeout timeout) {
    	if (timeout.getPeerId() == coordinatorId) {
	    	node.logMessage(timeout);
	        removeNodeWithIDFromUpset(timeout.getPeerId());
	        PeerReference lowestRemainingID = getNodeWithLowestIDInUpset();
	        Connection conn = node.connectTo(lowestRemainingID);
	        conn.sendMessage(new ElectedMessage(ongoingTransactionID));
    	}
    }

    private PeerReference getNodeWithLowestIDInUpset() {
        if (getUpSet().isEmpty()) return null;
        PriorityQueue<PeerReference> peerReferences = new PriorityQueue<>(getUpSet());
        return peerReferences.poll();
    }

    private void removeNodeWithIDFromUpset(int id) {
        upSet = upSet.stream().filter(c -> c.getNodeID() != id).collect(Collectors.toList());
    }
}
