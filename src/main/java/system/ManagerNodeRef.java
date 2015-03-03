package system;

import messages.Message;
import node.PeerReference;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import system.network.Connection;

/**
 * Ethan Petuchowski 2/16/15
 */
public class ManagerNodeRef {
    protected int nodeID;
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
        throw new NotImplementedException();
    }

    public Message receiveMessage() {
        return getConn().receiveMessage();
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
