package system;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Ethan Petuchowski 2/17/15
 */
public abstract class Node {

    /* CONSTANTS */
    protected static final int EXIT_FAILURE = 1;
    protected static final String LOCALHOST = "127.0.0.1";

    /* CONSTRUCTORS */
    public Node(int port) {
        portNum = port;
        startListening();
    }

    protected void startListening() {

    }

    /**
     * The Network will receive (e.g. "broadcast") messages from Nodes.
     * It will Delay each one as much as it wants to.
     * Then it will put it on a Node's inMsgQueue.
     * When the System tick()s a Node, that node will read-off all messages in its inMsgQueue
     *      via a socket connection with the Network.
     *
     */
    class Connector implements Runnable {

        ServerSocket serverSocket;

        public Connector() {
            try {
                serverSocket = new ServerSocket(portNum);
            }
            catch (IOException e) {
                e.printStackTrace();
            }
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


    /* FIELDS */
    protected Protocol protocol;
    protected int portNum;
    int logicalClock = 0;

    /* API */
    public void tick(String command) {
        logicalClock++;
    }


    /* PROTECTED METHODS */
    protected static void verifyArgs(String[] args) {
        if (args.length < 1) {
            System.err.println("Node requires a TCP port number to listen to. Exiting.");
            System.exit(EXIT_FAILURE);
        }
        try {
            int port = Integer.parseInt(args[0]);
            if (port < 1 || port > (int) Short.MAX_VALUE) {
                System.err.println("Port number must be in range [1,"+Short.MAX_VALUE+"]");
                System.exit(EXIT_FAILURE);
            }
        } catch (NumberFormatException e) {
            System.err.println("Port number invalid, must be digit: "+args[0]);
            System.exit(EXIT_FAILURE);
        }
    }
}
