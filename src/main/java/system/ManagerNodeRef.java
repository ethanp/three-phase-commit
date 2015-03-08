package system;

import messages.Message;
import node.PeerReference;
import system.network.Connection;
import util.Common;

import java.io.EOFException;

/**
 * Ethan Petuchowski 2/16/15
 */
public class ManagerNodeRef {
    private int nodeID;
    protected Connection conn;
    protected int listenPort;

    public ManagerNodeRef(int nodeID) {
        this.setNodeID(nodeID);
    }

    public void setConn(Connection conn) {
        this.conn = conn;
    }

    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }

    public int getListenPort() {
        return listenPort;
    }

    public int getNodeID() {
        return nodeID;
    }

    public void sendMessage(Message message) {
        getConn().sendMessage(message);
    }

    public void killNode() {
        throw new RuntimeException("You can only kill an AsyncManagerNodeRef!");
    }

    public Message receiveMessage() {
        try {
            return getConn().receiveMessage();
        }
        catch (EOFException ignored) {
            System.err.println("NodeRef "+getNodeID()+" received EOFException from "+getConn().getReceiverID());
            System.exit(Common.EXIT_FAILURE);
            return null;
        }
    }

    public PeerReference asPeerNode() {
        return new PeerReference(getNodeID(), getListenPort());
    }

    public Connection getConn() {
        return conn;
    }

    public void setNodeID(int nodeID) {
        this.nodeID = nodeID;
    }
}
