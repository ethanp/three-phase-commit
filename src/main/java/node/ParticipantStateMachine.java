package node;

import messages.Message;
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

    public void receiveInvalidVoteRequest(Message message) {
        node.sendMessage(new Message(Message.Command.NO, message.getMsgId()));
        node.log(message.getMsgId()+" ABORT");
    }
}
