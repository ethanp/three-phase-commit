package node.system;

import messages.Message;
import messages.NodeMessage;
import node.PeerReference;
import node.base.Node;
import system.network.Connection;
import system.network.ObjectConnection;
import util.Common;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import static util.Common.LOCALHOST;
import static util.Common.TXN_MGR_ID;


/**
 * Ethan Petuchowski 2/17/15
 */
public class AsyncProcessNode extends Node {

    NodeServer nodeServer;
    AsyncLogger L;

    AsyncProcessNode(int systemListenPort, int myNodeID) {
        super(myNodeID);
        dtLog = new FileDTLog(new File(Common.LOG_DIR, String.valueOf(myNodeID)), this);

        System.out.println("Node "+getMyNodeID()+": log on startup:");
        System.out.println(dtLog.getLogAsString());

        /* start local server */
        nodeServer = new NodeServer();
        L = new AsyncLogger(getMyNodeID(), getListenPort());
        new Thread(nodeServer).start();

        /* connect to System */
        try {
            final Socket socket = new Socket(Common.LOCALHOST, systemListenPort);
            txnMgrConn = new ObjectConnection(socket, TXN_MGR_ID);

            /* tell the System my logical ID and listen port */
            // (doesn't increment sent-msgs count)
            txnMgrConn.sendMessage(new NodeMessage(getMyNodeID(), getListenPort()));
            new Thread(new ConnectionListener(this, (ObjectConnection)txnMgrConn)).start();
        }
        catch (IOException e) {
            L.OG("couldn't establish connection to the System");
            System.exit(Common.EXIT_FAILURE);
        }

        recoverFromDtLog();
    }

    int getListenPort() {
        return nodeServer.getListenPort();
    }

    @Override public void addConnection(Connection connection) {
        super.addConnection(connection);
        if (connection == null) {
            System.err.println("Connection is null");
        }
        else {
            new Thread(new ConnectionListener(this, (ObjectConnection) connection)).start();
        }
    }

    /**
     * calling this method should make this peer acquire a connection to the referenced peer AND
     * should ('eventually') make that peer acquire a reciprocal connection back to this peer
     * <p>
     * In the synchronous case, this means directly adding each end of the QueueSocket to each
     * peer's `peerConns` collection
     * <p>
     * In the asynchronous case, it means (SYNCHRONOUSLY) establishing a socket with the referenced
     * peer's server
     */
    @Override public Connection connectTo(PeerReference peerReference) throws IOException {
        final ObjectConnection connection = new ObjectConnection(
                new Socket(LOCALHOST, peerReference.getListeningPort()),
                peerReference.getNodeID());
        addConnection(connection);
        connection.sendMessage(new NodeMessage(getMyNodeID(), getListenPort()));
        return connection;
    }

    @Override public void addTimerFor(int peerID) {
        timeoutMonitor.startTimer(peerID);
    }

    @Override public void selfDestruct() {
        System.err.println("Node "+getMyNodeID()+" self destructing!");
        System.exit(Common.EXIT_SUCCESS);
    }

    class NodeServer implements Runnable {

        ServerSocket serverSocket;

        int getListenPort() {
            return serverSocket.getLocalPort();
        }

        NodeServer() {
            serverSocket = Common.claimOpenPort();
        }

        @Override public void run() {
            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    final ObjectConnection connection = new ObjectConnection(socket, Common.INVALID_ID);
                    try {
                        Message msg = connection.receiveMessage();
                        if (msg instanceof NodeMessage) {
                            connection.setReceiverID(((NodeMessage) msg).getNodeID());
                            addConnection(connection);
                        }
                        else {
                            L.OG("Conn didn't receive node msg, connection failed");
                            connection.socket.close();
                        }
                    }
                    catch (EOFException e) {
                        L.OG("New conn received EOF");
                    }
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        int nodeID = args.length > 0 ? Integer.parseInt(args[0]) : 55;
        Common.ASYNC_NODE_ID = nodeID;
        int systemListenPort = args.length > 1 ? Integer.parseInt(args[1]) : 3000;
        System.out.println("Node "+nodeID+" booting in its own process");
        AsyncProcessNode node = new AsyncProcessNode(systemListenPort, nodeID);
    }
}
