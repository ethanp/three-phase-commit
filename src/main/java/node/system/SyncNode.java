package node.system;

import node.PeerReference;
import node.base.Node;
import node.mock.ByteArrayDTLog;
import system.SyncManagerNodeRef;
import system.SyncTxnMgr;
import system.network.Connection;
import system.network.QueueConnection;
import system.network.QueueSocket;

/**
 * Ethan Petuchowski 2/28/15
 */
public class SyncNode extends Node {

    int ticks = 0;

    public SyncNode(int myNodeID, QueueConnection toTxnManager) {
        this(myNodeID, toTxnManager, null);
    }

    public SyncNode(int myNodeID, QueueConnection toTxnManager, SyncTxnMgr txnMgr) {
        super(myNodeID);
        txnMgrConn = toTxnManager;
        dtLog = new ByteArrayDTLog(this);
        syncTxnMgr = txnMgr;
    }

    final SyncTxnMgr syncTxnMgr;

    /**
     * processes at most one incoming message
     * @return false if a message was processed,
     *         true if there were no messages to process
     */
    public boolean tick() {

        ticks++;
        if (receiveMessageFrom(txnMgrConn, ticks)) {
            return false;
        }

        for (Connection connection : peerConns) {
            if (receiveMessageFrom(connection, ticks)) {
                return false;
            }
        }

        return true;
    }

    /**
     * calling this method should make this peer acquire a connection to the referenced peer AND
     * should ('eventually') make that peer acquire a reciprocal connection back to this peer
     * <p>
     * In the synchronous case, this means directly adding each end of the QueueSocket to each
     * peer's `peerConns` collection
     * <p>
     * In the asynchronous case, it means establishing a socket with the referenced peer's server
     */
    @Override public Connection connectTo(PeerReference peerRef) {
        SyncManagerNodeRef n = (SyncManagerNodeRef)
                syncTxnMgr.getNodes()
                          .stream()
                          .filter(mgrNRef -> mgrNRef.getNodeID() == peerRef.getNodeID())
                          .findFirst()
                          .get();

        Node toConnectTo = n.getNode();
        QueueSocket qs = new QueueSocket(this.getMyNodeID(), toConnectTo.getMyNodeID());
        final QueueConnection connectionToPeer = qs.getConnectionToBID();
        this.addConnection(connectionToPeer);
        toConnectTo.addConnection(qs.getConnectionToAID());
        return connectionToPeer;
    }

    @Override protected void selfDestruct() {
        System.err.println("This is where I WOULD self-destruct.");
    }
}
