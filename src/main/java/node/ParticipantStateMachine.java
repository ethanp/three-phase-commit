package node;

import messages.AddRequest;
import messages.DeleteRequest;
import messages.Message;
import messages.NoResponse;
import messages.UpdateRequest;
import messages.YesResponse;
import node.base.Node;
import node.base.StateMachine;

/**
 * Ethan Petuchowski 2/27/15
 */
public class ParticipantStateMachine extends StateMachine {
    final Node node;
    public ParticipantStateMachine(Node node) {
        this.node = node;
    }

    /** log ABORT and send NO */
    public void respondNo(Message message) {
        node.log(message.getMsgId()+" "+Message.Command.ABORT);
        node.sendMessage(new NoResponse(message));
    }

    public void receiveVoteRequest(Message msg) {
        switch (msg.getCommand()) {
            case ADD:
                receiveAddRequest((AddRequest) msg);
                break;
            case UPDATE:
                receiveUpdateRequest((UpdateRequest) msg);
                break;
            case DELETE:
                receiveDeleteRequest((DeleteRequest) msg);
                break;
            default:
                throw new RuntimeException("Not a vote request: "+msg.getCommand());
        }
    }

    private void receiveDeleteRequest(DeleteRequest deleteRequest) {
        if (node.hasSong(deleteRequest.getSongName()))
            respondYes(deleteRequest);
        else
            respondNo(deleteRequest);
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

    private void respondYes(Message message) {
        logAndSend(new YesResponse(message));
    }

    private void logAndSend(Message message) {
        node.log(message);
        node.sendMessage(message);
    }
}
