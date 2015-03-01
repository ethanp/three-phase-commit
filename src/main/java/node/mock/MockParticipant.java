package node.mock;

import messages.Message;
import node.base.Node;
import node.ParticipantStateMachine;

import java.util.ArrayList;
import java.util.List;

/**
 * Ethan Petuchowski 2/27/15
 *
 * still has a state machine but doesn't try to establish
 * a Socket connection to the TransactionManager
 */
public class MockParticipant extends Node {
    public MockParticipant() {
        super(1);
        setDtLog(new ByteArrayDTLog(this));
    }

    List<Message> sentMessages = new ArrayList<>();

    public ParticipantStateMachine getStateMachine() {
        return (ParticipantStateMachine) stateMachine;
    }

    public List<Message> getSentMessages() {
        return sentMessages;
    }

    @Override public void sendCoordinatorMessage(Message message) {
        sentMessages.add(message);
    }
}
