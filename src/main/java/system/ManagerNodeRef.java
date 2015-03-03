package system;

import messages.Message;
import node.PeerReference;
import system.network.Connection;

/**
 * Ethan Petuchowski 2/16/15
 */
public class ManagerNodeRef {
    int nodeID;
    Connection conn;
    int listenPort;

    public ManagerNodeRef(int nodeID) {
        this.nodeID = nodeID;
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
        conn.sendMessage(message);
    }

    public void killNode() {

    }

    public Message receiveMessage() {
        return conn.receiveMessage();
    }

    public PeerReference asPeerNode() {
        return new PeerReference(getNodeID(), getListenPort());
    }
}
