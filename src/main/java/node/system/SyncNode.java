package node.system;

import node.base.Node;
import node.mock.ByteArrayDTLog;
import system.network.Connection;
import system.network.QueueConnection;

/**
 * Ethan Petuchowski 2/28/15
 */
public class SyncNode extends Node {
    public SyncNode(int myNodeID, QueueConnection toTxnManager) {
        super(myNodeID);
        txnMgrConn = toTxnManager;
        dtLog = new ByteArrayDTLog(this);
    }

    /**
     * processes at most one incoming message
     * @returns false if a message was processed
     * @returns true if there were no messages to process
     */
    public boolean tick() {

        if (receiveMessageFrom(txnMgrConn)) {
            return false;
        }

        for (Connection connection : peerConns) {
            if (receiveMessageFrom(connection)) {
                return false;
            }
        }

        return true;
    }


}
