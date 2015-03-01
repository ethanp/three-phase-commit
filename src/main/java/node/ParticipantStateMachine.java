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
                break;
            default:
                throw new RuntimeException("Not a valid message: "+msg.getCommand());
        }

        return true;
    }

    private void receiveDubCoordinator(Message message) {
        node.becomeCoordinator();
    }

    private void receiveAddRequest(AddRequest addRequest) {
        node.log(addRequest.toLogString());

        if (node.hasSong(addRequest.getSongTuple()))
            respondNoToVoteRequest(addRequest);
        else
            respondYesToVoteRequest(addRequest);
    }

    private void receiveUpdateRequest(UpdateRequest updateRequest) {
        if (!node.hasSong(updateRequest.getSongName()))
            respondNoToVoteRequest(updateRequest);
        else
            respondYesToVoteRequest(updateRequest);
    }

    private void receiveDeleteRequest(DeleteRequest deleteRequest) {
        if (node.hasSong(deleteRequest.getSongName()))
            respondYesToVoteRequest(deleteRequest);
        else
            respondNoToVoteRequest(deleteRequest);
    }

    /**
     * doesn't log anything
     */
    private void receivePrecommit(PrecommitRequest precommitRequest) {
        setPrecommitted(true);
    }

    private void receiveCommit(CommitRequest commitRequest) {
//        node.log(commitRequest);

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

    public void respondNoToVoteRequest(Message message) {
        setOngoingTransactionID(NO_ONGOING_TRANSACTION);
        node.log(message.getTransactionID()+" "+Message.Command.ABORT);
        currentConnection.sendMessage(new NoResponse(message));
    }

    private void respondYesToVoteRequest(VoteRequest voteRequest) {
        action = voteRequest;
        setOngoingTransactionID(voteRequest.getTransactionID());
        setPeerSet(voteRequest.getPeerSet());
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

    public Collection<PeerReference> getWorkingPeerSet() {
        return peerSet;
    }

    public void setPeerSet(Collection<PeerReference> peerSet) {
        this.peerSet = peerSet;
    }
}
