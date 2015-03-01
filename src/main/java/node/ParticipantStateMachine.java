package node;

import messages.CommitRequest;
import messages.Message;
import messages.NoResponse;
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
import java.util.Iterator;
import java.util.PriorityQueue;

import static messages.Message.Command.UR_ELECTED;
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
    private Collection<PeerReference> peerSet = null;
    private Collection<PeerReference> upSet = null;
    private boolean precommitted = false;
    private Connection currentConnection = null;

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
            default:
                throw new RuntimeException("Not a valid message: "+msg.getCommand());
        }

        return true;
    }

    private void receiveUR_ELECTED(Message message) {
        node.becomeCoordinator();
    }

    private void receiveAbort(Message message) {
        node.log("ABORT");
        action = null;
        setPeerSet(null);
        setUpSet(null);
        ongoingTransactionID = NO_ONGOING_TRANSACTION;
    }

    private void receiveDubCoordinator(Message message) {
        node.becomeCoordinator();
    }

    private void receiveVoteRequest(VoteRequest voteRequest, boolean voteValue) {
        node.log(voteRequest.toLogString());
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
                new Message(
                        Message.Command.ACK,
                        getOngoingTransactionID()));
    }

    private void receiveCommit(CommitRequest commitRequest) {
        node.log("COMMIT");

        switch (action.getCommand()) {
            case ADD:
                commitAdd((AddRequest) action);
                break;
            case UPDATE:
                commitUpdate((UpdateRequest) action);
                break;
            case DELETE:
                commitDelete((DeleteRequest) action);
                break;
        }
        setOngoingTransactionID(NO_ONGOING_TRANSACTION);
    }

    private void commitDelete(DeleteRequest deleteRequest) {
        node.removeSongWithName(deleteRequest.getSongName());
    }

    private void commitUpdate(UpdateRequest updateRequest) {
        node.updateSong(updateRequest.getSongName(), updateRequest.getUpdatedSong());
    }

    private void commitAdd(AddRequest addRequest) {
        node.addSong(addRequest.getSongTuple());
    }

    public void respondNOToVoteRequest(Message message) {
        setOngoingTransactionID(NO_ONGOING_TRANSACTION);
        node.log(message.getTransactionID()+" "+Message.Command.ABORT);
        currentConnection.sendMessage(new NoResponse(message));
    }

    private void respondYESToVoteRequest(VoteRequest voteRequest) {
        action = voteRequest;
        setOngoingTransactionID(voteRequest.getTransactionID());
        setPeerSet(voteRequest.getCloneOfPeerSet());
        setUpSet(voteRequest.getCloneOfPeerSet());
        logAndSend(new YesResponse(voteRequest));
    }

    private void logAndSend(Message message) {
        node.log(message.getCommand().toString());
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

    public Collection<PeerReference> getPeerSet() {
        return peerSet;
    }

    public void setPeerSet(Collection<PeerReference> peerSet) {
        this.peerSet = peerSet;
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

    public void coordinatorTimeoutOnHeartbeat(int coordinatorID) {
        node.log("TIMEOUT "+coordinatorID);
        removeNodeWithIDFromUpset(coordinatorID);
        PeerReference lowestRemainingID = getNodeWithLowestIDInUpset();
        Connection conn = node.connectTo(lowestRemainingID);
        conn.sendMessage(new Message(UR_ELECTED, getOngoingTransactionID()));
    }

    private PeerReference getNodeWithLowestIDInUpset() {
        if (getUpSet().isEmpty()) return null;
        PriorityQueue<PeerReference> peerReferences = new PriorityQueue<>(getUpSet());
        return peerReferences.poll();
    }

    private void removeNodeWithIDFromUpset(int id) {
        PeerReference toRemove = null;
        Iterator<PeerReference> referenceIterator = getUpSet().iterator();
        while (referenceIterator.hasNext()) {
            PeerReference ref = referenceIterator.next();
            if (ref.getNodeID() == id) {
                toRemove = ref;
                break;
            }
        }
        if (toRemove != null) {
            getUpSet().remove(toRemove);
        }
    }
}
