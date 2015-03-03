package system;

import messages.Message;
import system.network.QueueSocket;
import util.Common;

import java.util.ArrayList;
import java.util.List;

import static messages.Message.Command.ABORT;
import static messages.Message.Command.COMMIT;

/**
 * Ethan Petuchowski 2/28/15
 */
public class SyncTxnMgr extends TransactionManager {
    public SyncTxnMgr(int numNodes) {
        super(numNodes);
        dubCoordinator(1);
    }

    @Override public List<ManagerNodeRef> createNodes(int numNodes) {
        List<ManagerNodeRef> list = new ArrayList<>();
        for (int i = 1; i <= numNodes ; i++) {
            QueueSocket socket = new QueueSocket(Common.TXN_MGR_ID, i);
            list.add(new SyncManagerNodeRef(i, socket, this));
        }
        return list;
    }


    /**
     * @return true if received COMMIT, or ABORT
     */
    public boolean tick() {
        for (ManagerNodeRef nodeRef : nodes) {
            Message message = nodeRef.receiveMessage();
            if (message != null) {
                if (message.getCommand() == COMMIT || message.getCommand() == ABORT) {
                    return true;
                }
            }
        }
        return false;
    }
}
