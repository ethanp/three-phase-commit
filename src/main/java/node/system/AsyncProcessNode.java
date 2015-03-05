package node.system;

import messages.NodeMessage;
import node.PeerReference;
import node.base.Node;
import system.network.Connection;
import system.network.ObjectConnection;
import util.Common;

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
        dtLog = new FileDTLog(new File("logDir", String.valueOf(myNodeID)), this);

        /* start local server */
        nodeServer = new NodeServer();
        L = new AsyncLogger(getMyNodeID(), getListenPort());
        L.OG("Server starting");
        new Thread(nodeServer).start();

        /* connect to System */
        try {
            L.OG("establishing socket connection to system at port "+systemListenPort);
            final Socket socket = new Socket(Common.LOCALHOST, systemListenPort);
            L.OG("establishing object connection to system at port "+systemListenPort);
            txnMgrConn = new ObjectConnection(socket, TXN_MGR_ID);
            L.OG("connected to System");

            /* tell the System my logical ID and listen port */
            txnMgrConn.sendMessage(new NodeMessage(getMyNodeID(), getListenPort()));
            new Thread(new ConnectionListener(this, (ObjectConnection)txnMgrConn)).start();
        }
        catch (IOException e) {
            L.OG("couldn't establish connection to the System");
            System.exit(Common.EXIT_FAILURE);
        }
    }

    int getListenPort() {
        return nodeServer.getListenPort();
    }

    @Override public void addConnection(Connection connection) {
        super.addConnection(connection);
        new Thread(new ConnectionListener(this, (ObjectConnection)connection)).start();
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
    @Override public Connection connectTo(PeerReference peerReference) {
        try {
            final ObjectConnection connection = new ObjectConnection(
                    new Socket(LOCALHOST, peerReference.getListeningPort()),
                    peerReference.getNodeID());
            addConnection(connection);
            return connection;
        }
        catch (IOException e) {
            System.err.println("Couldn't connect to peer "+peerReference.getNodeID()+" "+
                               "on port "+peerReference.getListeningPort());
            e.printStackTrace();
            return null;
        }
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
                    addConnection(new ObjectConnection(socket, Common.INVALID_ID));
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        int systemListenPort = args.length > 0 ? Integer.parseInt(args[0]) : 3000;
        int nodeID = args.length > 1 ? Integer.parseInt(args[1]) : 55;
        System.out.println("Node "+nodeID+" booting in its own process");
        AsyncProcessNode node = new AsyncProcessNode(systemListenPort, nodeID);
    }
}
