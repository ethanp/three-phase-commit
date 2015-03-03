package system;

import messages.Message;
import messages.PeerTimeout;
import messages.vote_req.VoteRequest;

/**
 * Ethan Petuchowski 2/28/15
 *
 * ticks all members of the system
 */
public class SynchronousSystem extends DistributedSystem {
    final SyncTxnMgr txnMgr;
    public static final int MAX_TICKS = 200;

    public SynchronousSystem(int numNodes) {
        txnMgr = new SyncTxnMgr(numNodes);
    }

    @Override Message processRequestToCompletion(VoteRequest voteRequest) {
        Message result;
        boolean noTick;
        txnMgr.processRequest(voteRequest);
        for (int i = 0; i < MAX_TICKS; i++) {
            noTick = true;
            result = txnMgr.tick();
            for (ManagerNodeRef node : txnMgr.getNodes()) {
                noTick &= ((SyncManagerNodeRef) node).tick();
            }
            if (noTick && result != null)
                return result;
        }
        System.err.println("Did not complete in "+MAX_TICKS+" ticks");
        return new PeerTimeout(-1);
    }
}
