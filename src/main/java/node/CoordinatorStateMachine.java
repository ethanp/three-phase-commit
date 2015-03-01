package node;

import node.base.StateMachine;
import system.network.Connection;

/**
 * Ethan Petuchowski 2/28/15
 */
public class CoordinatorStateMachine extends StateMachine {
    @Override public boolean receiveMessage(Connection overConnection) {
        switch (overConnection.receiveMessage().getCommand()) {

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
        return true;
    }
}
