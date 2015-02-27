package system;

import system.network.ObjectConnection;

/**
 * Ethan Petuchowski 2/16/15
 */
public class SystemNodeReference {
    Process process;
    int id;
    ObjectConnection conn;
    int listenPort;

    public SystemNodeReference(int nodeID, Process process) {
        this.id = nodeID;
        this.process = process;
    }

    public void setConn(ObjectConnection conn) {
        this.conn = conn;
    }

    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }

    public int getID() {
        return id;
    }
}
