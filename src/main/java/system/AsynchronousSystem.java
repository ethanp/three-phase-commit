package system;

import messages.Message;
import messages.vote_req.VoteRequest;

/**
 * Ethan Petuchowski 3/2/15
 */
public class AsynchronousSystem extends DistributedSystem {
    final AsyncTxnMgr txnMgr;

    public AsynchronousSystem(int numNodes) {
        this.txnMgr = new AsyncTxnMgr(numNodes);
    }

    @Override Message processRequestToCompletion(VoteRequest voteRequest) {
        txnMgr.processRequest(voteRequest);
        try {

            /**
             * Acquires the lock *unless* the current thread is `interrupted`.
             * Acquires the lock if it is available and returns immediately.
             *
             * If the lock is not available then the current thread becomes disabled for
             *      thread scheduling purposes and lies dormant until one of two things happens:
             *
             *          1. The lock is acquired by the current thread; or
             *
             *          2. Some other thread interrupts the current thread,
             *              and interruption of lock acquisition is supported.
             */
            txnMgr.transactionLock.lockInterruptibly();

            /**
             * Causes the current thread to wait until it is signalled or interrupted.
             *
             * The lock associated with this Condition is atomically released
             *      and the current thread becomes disabled for thread scheduling purposes
             *      and lies dormant until some other thread invokes the signalAll()
             *      method for this Condition or some other thread interrupts the current thread.
             *
             * Before this method can return the current thread re-acquires the lock associated
             *  with this condition, so when the thread returns it is guaranteed to hold this lock.
             */
            txnMgr.transactionComplete.await();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        finally {
            txnMgr.transactionLock.unlock();
        }
        return txnMgr.getTransactionResult();
    }

    void killAllNodes() {
        /* kill the nodes */
        txnMgr.getNodes().stream().forEach(ManagerNodeRef::killNode);
    }
}
