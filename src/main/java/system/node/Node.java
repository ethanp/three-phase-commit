package system.node;

import system.Protocol;
import system.network.ObjectConnection;
import util.Common;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

/**
 * Ethan Petuchowski 2/17/15
 */
public class Node {

    Protocol protocol;
    private int myNodeID;
    ObjectConnection systemConnection;
    StateMachine stateMachine;
    NodeServer nodeServer;
    DTLog log;
    Map<String, String> playlist = new HashMap<>();

    Node(int systemListenPort, int myNodeID) {
        this.setMyNodeID(myNodeID);
        protocol = new ParticipantProtocol();
        log = new DTLog(this, new File("logDir", String.valueOf(myNodeID)));

        /* start local server */
        nodeServer = new NodeServer();
        new Thread(nodeServer).start();

        /* connect to System */
        try {
            systemConnection = new ObjectConnection(new Socket(Common.LOCALHOST, systemListenPort));
            System.out.println("Node "+myNodeID+" connected to System");

            /* tell the System my ID then my listen port */
            systemConnection.out.writeInt(myNodeID);
            systemConnection.out.writeInt(nodeServer.getListenPort());
        }
        catch (IOException e) {
            System.err.println("Node "+myNodeID+" couldn't establish connection to the System");
            System.exit(Common.EXIT_FAILURE);
        }
    }

    public int getMyNodeID() {
        return myNodeID;
    }

    public void setMyNodeID(int myNodeID) {
        this.myNodeID = myNodeID;
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
        Node node = new Node(systemListenPort, nodeID);
    }
}
