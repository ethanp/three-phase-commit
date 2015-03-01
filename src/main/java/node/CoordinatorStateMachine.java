package node;

import messages.Message;
import node.base.StateMachine;

/**
 * Ethan Petuchowski 2/28/15
 */
public class CoordinatorStateMachine extends StateMachine {
    @Override public void receiveMessage(Message message) {
        switch (message.getCommand()) {

            case DUB_COORDINATOR:
                break;
            case VOTE_REQ:
                break;
            case PRE_COMMIT:
                break;
            case COMMIT:
                break;
            case ABORT:
                break;
            case YES:
                break;
            case NO:
                break;
            case ACK:
                break;
            case ADD:
                break;
            case UPDATE:
                break;
            case DELETE:
                break;
        }

    }
}
