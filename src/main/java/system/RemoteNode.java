package system;

import system.network.ObjectConnection;

/**
 * Ethan Petuchowski 2/16/15
 */
public class RemoteNode {
    Process process;
    int nodeID;
    ObjectConnection conn;

    public Process getProcess() { return process; }

    public RemoteNode(int nodeID, Process process) {
        this.nodeID = nodeID;
        this.process = process;
    }
}
