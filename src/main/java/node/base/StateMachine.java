package node.base;

import system.network.Connection;

/**
 * Ethan Petuchowski 2/26/15
 */
public abstract class StateMachine {
    public abstract boolean receiveMessage(Connection overConnection);
}
