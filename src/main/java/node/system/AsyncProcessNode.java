package node.system;

import messages.NodeMessage;
import node.PeerReference;
import node.base.Node;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import system.network.Connection;
import system.network.ObjectConnection;
import util.Common;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import static util.Common.TXN_MGR_ID;


/**
 * Ethan Petuchowski 2/17/15
 */
public class AsyncProcessNode extends Node {

    NodeServer nodeServer;

    AsyncProcessNode(int systemListenPort, int myNodeID) {
        super(myNodeID);
        dtLog = new FileDTLog(new File("logDir", String.valueOf(myNodeID)), this);

        /* start local server */
        nodeServer = new NodeServer();
        new Thread(nodeServer).start();

        /* connect to System */
        try {
            txnMgrConn = new ObjectConnection(new Socket(Common.LOCALHOST, systemListenPort), TXN_MGR_ID);
            System.out.println("Node "+myNodeID+" connected to System");

            /* tell the System my logical ID and listen port */
            txnMgrConn.sendMessage(new NodeMessage(getMyNodeID(), getListenPort()));
        }
        catch (IOException e) {
            System.err.println("Node "+myNodeID+" couldn't establish connection to the System");
            System.exit(Common.EXIT_FAILURE);
        }
    }

    int getListenPort() {
        return nodeServer.getListenPort();
    }

    @Override public void addConnection(Connection connection) {
        super.addConnection(connection);
        new Thread(new ConnectionListener((ObjectConnection)connection)).start();
    }

    class ConnectionListener implements Runnable {
        ObjectConnection connection;
        ConnectionListener(ObjectConnection connection) {
            this.connection = connection;
        }

        @Override public void run() {
            while (true) {
                while (!connection.messageWaiting()) try {
                    Thread.sleep(Common.MESSAGE_DELAY);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                receiveMessageFrom(connection);
            }
        }
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
    @Override public Connection connectTo(PeerReference peerReference) {
        throw new NotImplementedException();
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
        System.out.println("Node "+nodeID);
        AsyncProcessNode node = new AsyncProcessNode(systemListenPort, nodeID);
    }
}
