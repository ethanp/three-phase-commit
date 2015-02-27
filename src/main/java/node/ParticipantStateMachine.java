package node;

import messages.AddRequest;
import messages.DeleteRequest;
import messages.Message;
import messages.NoResponse;
import messages.PrecommitRequest;
import messages.UpdateRequest;
import messages.YesResponse;
import node.base.Node;
import node.base.StateMachine;

/**
 * Ethan Petuchowski 2/27/15
 */
public class ParticipantStateMachine extends StateMachine {

    /* CONSTANTS */
    public final static int NO_ONGOING_TRANSACTION = -1;

    /* REFERENCES */
    final Node node;

    /* ATTRIBUTES */
    protected int ongoingTransactionID = NO_ONGOING_TRANSACTION;
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

    /** log ABORT and send NO */
    public void respondNo(Message message) {
        setOngoingTransactionID(NO_ONGOING_TRANSACTION);
        node.log(message.getTransactionID()+" "+Message.Command.ABORT);
        node.sendMessage(new NoResponse(message));
    }

    private void respondYes(Message message) {
        setOngoingTransactionID(message.getTransactionID());
        logAndSend(new YesResponse(message));
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

    private void receivePrecommit(PrecommitRequest msg) {
        if (ongoingTransactionID != msg.getTransactionID()) {
            /* TODO what do we do here? */
        }
        setPrecommitted(true);
    }

    public boolean isPrecommitted() {
        return precommitted;
    }

    public void setPrecommitted(boolean precommitted) {
        this.precommitted = precommitted;
    }
}
