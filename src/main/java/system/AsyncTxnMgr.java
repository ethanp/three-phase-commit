package system;

import node.system.AsyncLogger;
import node.system.AsyncProcessNode;
import util.Common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Ethan Petuchowski 2/28/15
 */
public class AsyncTxnMgr extends TransactionManager {
    public AsyncTxnMgr(int numNodes) {
        super(numNodes);
        waitForAllNodesToConnect();
        dubCoordinator(nodes.get(0).getNodeID());
        nodesConnected.lock();
        coordinatorChosen.signalAll();
        nodesConnected.unlock();
    }

    private void waitForAllNodesToConnect() {
        while (getNumConnectedNodes() < getNodes().size()) {
            try {
                nodesConnected.lock();
                allNodesConnected.await();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            finally {
                nodesConnected.unlock();
            }
        }
    }

    final Lock nodesConnected = new ReentrantLock();
    final Condition allNodesConnected = nodesConnected.newCondition();
    final Condition coordinatorChosen = nodesConnected.newCondition();
    protected TxnMgrServer mgrServer;
    protected AsyncLogger L;

    long getNumConnectedNodes() {
        return nodes.stream().filter(n -> n.getConn() != null).count();
    }

    @Override public List<ManagerNodeRef> createNodes(int numNodes) {

        startServer();

        final List<String> commandLine = Arrays.asList(
                "java", "-cp", "target/classes", AsyncProcessNode.class.getCanonicalName(),
                String.valueOf(mgrServer.getListenPort()), "fakeID");

        /* The start() method creates a new Process instance with those attributes.
           The start() method can be invoked repeatedly from the same instance to
                create new subprocesses with identical or related attributes. */
        final ProcessBuilder procBldr = new ProcessBuilder(commandLine);

        /* merge location of STDOUT and STDERR */
        procBldr.redirectErrorStream(true);

        /* set subprocess STDOUT to the same as the current process */
        procBldr.redirectOutput(ProcessBuilder.Redirect.INHERIT);

        final List<ManagerNodeRef> nodes = new ArrayList<>();
        for (int nodeID = 1; nodeID <= numNodes; nodeID++) {
            Process p = null;
            int retries = 0;
            while (p == null && retries++ < 3) {
                try {
                    int lastIdx = procBldr.command().size() - 1;
                    procBldr.command().set(lastIdx, String.valueOf(nodeID));
                    p = procBldr.start();
                    nodes.add(new AsyncManagerNodeRef(nodeID, p));
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return nodes;
    }

    private void startServer() {
        mgrServer = new TxnMgrServer(this);
        L = new AsyncLogger(Common.TXN_MGR_ID, mgrServer.getListenPort());
        L.OG("Server started");
        new Thread(mgrServer).start();
    }
}
