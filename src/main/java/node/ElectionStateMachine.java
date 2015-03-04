package node;

import node.base.StateMachine;
import system.network.Connection;

/**
 * Ethan Petuchowski 3/3/15
 */
public class ElectionStateMachine extends StateMachine {
    @Override public boolean receiveMessage(Connection overConnection) {
        return false;
    }
}
