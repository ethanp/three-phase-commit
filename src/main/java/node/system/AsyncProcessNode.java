package node.system;

import node.base.Node;
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

            /* tell the System my ID then my listen port */
//            connection.sendMessage(new Integer(myNodeID));
//            connection.out.writeInt(nodeServer.getListenPort());
        }
        catch (IOException e) {
            System.err.println("Node "+myNodeID+" couldn't establish connection to the System");
            System.exit(Common.EXIT_FAILURE);
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
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        int systemListenPort = Integer.parseInt(args[0]);
        int nodeID = Integer.parseInt(args[1]);
        System.out.println("Node "+nodeID);
        AsyncProcessNode node = new AsyncProcessNode(systemListenPort, nodeID);
    }
}
