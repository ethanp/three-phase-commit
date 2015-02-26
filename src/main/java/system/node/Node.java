package system.node;

import system.Protocol;
import system.network.ObjectConnection;
import util.Common;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Ethan Petuchowski 2/17/15
 */
public class Node {
    Protocol protocol;
    int myNodeID;
    ObjectConnection systemConnection;
    ServerSocket serverSocket;

    Node(int systemListenPort, int myNodeID) {
        this.myNodeID = myNodeID;
        protocol = new ParticipantProtocol();
        serverSocket = Common.claimOpenPort();
        try {
            systemConnection = new ObjectConnection(new Socket(Common.LOCALHOST, systemListenPort));
            System.out.println("Node "+myNodeID+" connected to System");
        }
        catch (IOException e) {
            System.err.println("Node "+myNodeID+" couldn't establish connection to the System");
            System.exit(Common.EXIT_FAILURE);
        }
    }


    public static void main(String[] args) throws IOException {
        int systemListenPort = Integer.parseInt(args[0]);
        int nodeID = Integer.parseInt(args[1]);
        Node node = new Node(systemListenPort, nodeID);
    }

    class NodeServer implements Runnable {
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
}
