package system;

import messages.DecisionRequest;
import messages.Message;
import messages.NodeMessage;
import node.system.ConnectionListener;
import system.network.Connection;
import system.network.MessageReceiver;
import system.network.ObjectConnection;
import util.Common;

import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Ethan Petuchowski 2/26/15
 */
public class TxnMgrServer implements Runnable, MessageReceiver {
    AsyncTxnMgr txnMgr;
    ServerSocket serverSocket;
    static int requestPort = 3000;
    private boolean waitForCoordinatorToReconnectThenSendDecisionRequest = false;

    public int getListenPort() {
        return serverSocket.getLocalPort();
    }

    public TxnMgrServer(AsyncTxnMgr txnMgr) {
        this.txnMgr = txnMgr;
        serverSocket = Common.claimOpenPort();
    }

    @Override public void run() {
        while (true) {
            try {
                Socket socket = serverSocket.accept();
                ObjectConnection conn = addConnection(socket);

                /* transactions can commence once all nodes have connected */
                txnMgr.nodesConnected.lockInterruptibly();
                if (txnMgr.getNumConnectedNodes() == txnMgr.getNodes().size()) {
                    txnMgr.allNodesConnected.signalAll();
                }
                txnMgr.nodesConnected.unlock();

                if (waitForCoordinatorToReconnectThenSendDecisionRequest) {
                    txnMgr.sendCoordinator(new DecisionRequest(txnMgr.getTransactionID()));
                }
            }
            catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    ObjectConnection addConnection(Socket socket) {
        ObjectConnection conn = new ObjectConnection(socket, -1);

        NodeMessage n = null;
        try {
            n = (NodeMessage) conn.receiveMessage();
        }
        catch (EOFException ignored) {
//            System.err.println("TxnMgr received EOFException from "+conn.getReceiverID());
        }
        assert n != null;
        int nodeID = n.getNodeID();
        int listenPort = n.getListenPort();

        conn.setReceiverID(nodeID);
        ManagerNodeRef mgrNodeRef = txnMgr.remoteNodeWithID(nodeID);
        mgrNodeRef.setListenPort(listenPort);
        mgrNodeRef.setConn(conn);
        new Thread(new ConnectionListener(this, conn)).start();
        return conn;
    }

    /**
     * This is the callback called by the `ConnectionListener` when a
     * `Message` is received over the associated `Connection`.
     * It should inform the User of the outcome of their submitted `VoteRequest`.
     */
    @Override public boolean receiveMessageFrom(Connection connection, int msgsRcvd) {
        try {
            final Message message = connection.receiveMessage();
            final int nodeID = connection.getReceiverID();
            if (message == null) {
                System.err.println("TxnMgrServer received a null message from "+nodeID);
                txnMgr.reviveNode(nodeID);

                /* if it was the coordinator who died, */
                if (nodeID == txnMgr.getCoordinator().getNodeID() && txnMgr.getTransactionResult() == null) {
                    waitForCoordinatorToReconnectThenSendDecisionRequest = true;
                }
            }
            else {
                System.out.println("mgr rcvd a "+message.getCommand()+" from node "+nodeID);
                txnMgr.receiveResponse(message);
            }
        }
        catch (EOFException ignore) {
//            System.err.println("TxnMgr received EOFException from "+connection.getReceiverID());
        }
        return true;
    }
}
