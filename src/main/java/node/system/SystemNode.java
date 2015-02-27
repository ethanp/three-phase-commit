package node.system;

import messages.Message;
import node.ParticipantProtocol;
import node.base.Node;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import system.Protocol;
import system.network.ObjectConnection;
import util.Common;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;


/**
 * Ethan Petuchowski 2/17/15
 */
public class SystemNode extends Node {

    Protocol protocol;
    ObjectConnection tmConnection;
    NodeServer nodeServer;

    SystemNode(int systemListenPort, int myNodeID) {
        super(myNodeID);
        protocol = new ParticipantProtocol();
        dtLog = new FileDTLog(new File("logDir", String.valueOf(myNodeID)), this);

        /* start local server */
        nodeServer = new NodeServer();
        new Thread(nodeServer).start();

        /* connect to System */
        try {
            tmConnection = new ObjectConnection(new Socket(Common.LOCALHOST, systemListenPort));
            System.out.println("Node "+myNodeID+" connected to System");

            /* tell the System my ID then my listen port */
            tmConnection.out.writeInt(myNodeID);
            tmConnection.out.writeInt(nodeServer.getListenPort());
        }
        catch (IOException e) {
            System.err.println("Node "+myNodeID+" couldn't establish connection to the System");
            System.exit(Common.EXIT_FAILURE);
        }
    }

    @Override public void sendMessage(Message message) {
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
        SystemNode node = new SystemNode(systemListenPort, nodeID);
    }
}
