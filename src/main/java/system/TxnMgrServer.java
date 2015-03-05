package system;

import messages.Message;
import messages.NodeMessage;
import node.system.ConnectionListener;
import system.network.Connection;
import system.network.MessageReceiver;
import system.network.ObjectConnection;
import util.Common;

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
            }
            catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    ObjectConnection addConnection(Socket socket) {
        ObjectConnection conn = new ObjectConnection(socket, -1);

        NodeMessage n = (NodeMessage) conn.receiveMessage();
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
    @Override public boolean receiveMessageFrom(Connection connection) {
        final Message message = connection.receiveMessage();
        System.out.println("mgr rcvd a "+message.getCommand());
        txnMgr.receiveResponse(message);
        return true;
    }
}
