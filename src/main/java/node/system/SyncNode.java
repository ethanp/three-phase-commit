package node.system;

import node.PeerReference;
import node.base.Node;
import node.mock.ByteArrayDTLog;
import system.network.Connection;
import system.network.QueueConnection;
import system.network.QueueSocket;

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


    @Override public QueueConnection connectTo(PeerReference peerReference) {
        QueueSocket qs = new QueueSocket(getMyNodeID(), peerReference.getNodeID());
        final QueueConnection toBID = qs.getConnectionToBID();
        addConnection(toBID);
        return toBID;
    }
}
