package system;

import messages.vote_req.VoteRequest;

/**
 * Ethan Petuchowski 2/28/15
 *
 * ticks all members of the system
 */
public class SynchronousSystem {
    final SyncTxnMgr txnMgr;
    public static final int MAX_TICKS = 200;

    public SynchronousSystem(int numNodes) {
        txnMgr = new SyncTxnMgr(numNodes);
    }

    boolean handleRequest(VoteRequest voteRequest) {
        boolean result;
        txnMgr.processRequest(voteRequest);
        for (int i = 0; i < MAX_TICKS; i++) {
            result = true;
            result &= txnMgr.tick();
            for (ManagerNodeRef node : txnMgr.getNodes()) {
                result &= ((SyncManagerNodeRef) node).tick();
            }
            if (result)
                return true;
        }
        System.err.println("Did not complete in "+MAX_TICKS+" ticks");
        return false;
    }

}
