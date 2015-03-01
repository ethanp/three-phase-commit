package system;

import node.system.SyncNode;
import system.network.QueueSocket;

/**
 * Ethan Petuchowski 2/28/15
 */
public class SyncManagerNodeRef extends ManagerNodeRef {
    public SyncManagerNodeRef(int nodeID, QueueSocket conn) {
        super(nodeID);
        setConn(conn.getASide());
        node = new SyncNode(nodeID, conn.getBSide());
    }

    SyncNode node;

    public boolean tick() {
        return node.tick();
    }
}
