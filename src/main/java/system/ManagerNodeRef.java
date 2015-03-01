package system;

import messages.Message;
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

    public int getID() {
        return nodeID;
    }

    public void sendMessage(Message message) {
        conn.writeObject(message);
    }

    public void killNode() {

    }

    public Message receiveMessage() {
        return conn.readObject();
    }
}
