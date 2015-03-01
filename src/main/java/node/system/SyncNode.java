package node.system;

import messages.Message;
import node.base.Node;
import system.network.Connection;
import system.network.QueueConnection;

/**
 * Ethan Petuchowski 2/28/15
 */
public class SyncNode extends Node {
    public SyncNode(int myNodeID, QueueConnection toTxnManager) {
        super(myNodeID);
        txnMgrConn = toTxnManager;
    }

    @Override public void sendCoordinatorMessage(Message message) {

    }

    public boolean tick() {
        Message message;
        message = txnMgrConn.readObject();
        if (message != null) {
            receiveMessage(message);
            return false;
        }
        for (Connection connection : peerConns) {
            message = connection.readObject();
            if (message != null) {
                receiveMessage(message);
                return false;
            }
        }
        return true;
    }
}
