package system;

import messages.vote_req.VoteRequest;

/**
 * Ethan Petuchowski 3/2/15
 */
public class AsynchronousSystem extends DistributedSystem {
    final AsyncTxnMgr txnMgr;

    public AsynchronousSystem(int numNodes) {
        this.txnMgr = new AsyncTxnMgr(numNodes);
    }

    @Override boolean processRequestToCompletion(VoteRequest voteRequest) {
        try {
            txnMgr.processRequest(voteRequest);
            voteRequest.wait();
        }
        catch (InterruptedException e) {
            return false;
        }
        return true;
    }
}
