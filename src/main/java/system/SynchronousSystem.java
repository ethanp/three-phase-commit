package system;

import messages.vote_req.VoteRequest;

/**
 * Ethan Petuchowski 2/28/15
 *
 * ticks all members of the system
 */
public class SynchronousSystem {
    SyncTxnMgr txnMgr;

    public boolean getResult() {
        return result;
    }

    boolean result;

    SynchronousSystem(VoteRequest voteRequest) {
        txnMgr = new SyncTxnMgr(1);
        txnMgr.processRequest(voteRequest);
        while (true) {
            result = true;
            result &= txnMgr.tick();
            for (ManagerNodeRef node : txnMgr.getNodes()) {
                result &= ((SyncManagerNodeRef) node).tick();
            }
            if (result) return;
        }
    }

}
