package node;

import node.base.StateMachine;
import system.network.Connection;

/**
 * Ethan Petuchowski 3/3/15
 */
public class ElectionStateMachine extends StateMachine {
	// the first thing the ElectionStateMachine should do is seek out the node with the lowest id.
	// if the lowest id node is somebody else, send them a UR_ELECTED message, then expect to get a state request.
	// if they timeout, remove them from up set and repeat seeking.
	// if I'm the lowest node, then jump into Coordinator Termination Protocol.
    @Override public boolean receiveMessage(Connection overConnection) {
        return false;
    }
}
