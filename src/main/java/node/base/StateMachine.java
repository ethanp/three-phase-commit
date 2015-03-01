package node.base;

import messages.Message;

/**
 * Ethan Petuchowski 2/26/15
 */
public abstract class StateMachine {
    public abstract void receiveMessage(Message message);
}
