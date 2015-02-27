package node;

import messages.vote_req.AddRequest;
import messages.CommitRequest;
import messages.vote_req.DeleteRequest;
import messages.Message;
import messages.NoResponse;
import messages.PrecommitRequest;
import messages.vote_req.UpdateRequest;
import messages.YesResponse;
import messages.vote_req.VoteRequest;
import node.base.Node;
import node.base.StateMachine;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Collection;

/**
 * Ethan Petuchowski 2/27/15
 */
public class ParticipantStateMachine extends StateMachine {

    /* CONSTANTS */
    public final static int NO_ONGOING_TRANSACTION = -1;

    /* REFERENCES */
    final Node node;

    /* ATTRIBUTES */
    private int ongoingTransactionID = NO_ONGOING_TRANSACTION;
    private Collection<PeerReference> peerSet = null;
    private boolean precommitted = false;

    public ParticipantStateMachine(Node node) {
        this.node = node;
    }

    public void receiveMessage(Message msg) {
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
    }

    private void receiveAddRequest(AddRequest addRequest) {
        if (node.hasSong(addRequest.getSongTuple()))
            respondNo(addRequest);
        else
            respondYes(addRequest);
    }

    private void receiveUpdateRequest(UpdateRequest updateRequest) {
        if (!node.hasSong(updateRequest.getSongName()))
            respondNo(updateRequest);
        else
            respondYes(updateRequest);
    }

    private void receiveDeleteRequest(DeleteRequest deleteRequest) {
        if (node.hasSong(deleteRequest.getSongName()))
            respondYes(deleteRequest);
        else
            respondNo(deleteRequest);
    }

    private void receivePrecommit(PrecommitRequest precommitRequest) {
        if (ongoingTransactionID != precommitRequest.getTransactionID()) {
            /* TODO what do we do here? */
            throw new NotImplementedException();
        }
        setPrecommitted(true);
    }

    private void receiveCommit(CommitRequest commitRequest) {
        node.log(commitRequest);

        // TODO perform commit of whatever the request was supposed to DO
        throw new NotImplementedException();

//        setOngoingTransactionID(NO_ONGOING_TRANSACTION);
    }

    /** log ABORT and send NO */
    public void respondNo(Message message) {
        setOngoingTransactionID(NO_ONGOING_TRANSACTION);
        node.log(message.getTransactionID()+" "+Message.Command.ABORT);
        node.sendMessage(new NoResponse(message));
    }

    private void respondYes(VoteRequest voteRequest) {
        setOngoingTransactionID(voteRequest.getTransactionID());
        setPeerSet(voteRequest.getPeerSet());
        logAndSend(new YesResponse(voteRequest));
    }

    private void logAndSend(Message message) {
        node.log(message);
        node.sendMessage(message);
    }

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
