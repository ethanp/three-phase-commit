package system;

import console.CommandConsole;
import messages.KillSig;
import messages.Message;
import node.system.AsyncLogger;
import node.system.AsyncProcessNode;
import system.failures.Failure;
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
        console = new CommandConsole(this);
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

    final Lock transactionLock = new ReentrantLock();
    final Condition transactionComplete = transactionLock.newCondition();
    private Message transactionResult = null;

    protected TxnMgrServer mgrServer;
    protected AsyncLogger L;

    protected List<Failure> failures = new ArrayList<>();
    protected CommandConsole console;


    long getNumConnectedNodes() {
        return nodes.stream().filter(n -> n.getConn() != null).count();
    }

    public void processCommand(CommandConsole.Command command) {
        /**
         * Commands include
         *      1. Issuing a `VoteRequest`along with a `List<Failure>`
         *      2. Issuing a `KillSig` vote request
         */
        switch (command.getVoteRequest().getCommand()) {
            case KILL_SIG:
                KillSig killSig = (KillSig) command.getVoteRequest();
                ManagerNodeRef nodeToKill = remoteNodeWithID(killSig.getNodeID());
                restartNode(nodeToKill);
                break;
            default:
                processRequest(command.getVoteRequest());
        }

    }

    private void restartNode(ManagerNodeRef nodeToKill) {
        nodeToKill.killNode();
        createNode(nodeToKill.getNodeID());
    }

    public ManagerNodeRef createNode(int nodeID) {
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

        Process p = null;
        int retries = 0;
        //noinspection ConstantConditions
        while (p == null && retries++ < 3) {
            try {
                int lastIdx = procBldr.command().size() - 1;
                procBldr.command().set(lastIdx, String.valueOf(nodeID));
                p = procBldr.start();
                return new AsyncManagerNodeRef(nodeID, p);
            }
            catch (IOException e) {
                System.err.println("Failed to start node "+nodeID+", try "+retries);
            }
        }
        System.err.println("Couldn't start node "+nodeID);
        return null;
    }

    @Override public List<ManagerNodeRef> createNodes(int numNodes) {

        startServer();

        final List<ManagerNodeRef> nodes = new ArrayList<>();
        for (int nodeID = 1; nodeID <= numNodes; nodeID++) {
            nodes.add(createNode(nodeID));
        }

        return nodes;
    }

    private void startServer() {
        mgrServer = new TxnMgrServer(this);
        L = new AsyncLogger(Common.TXN_MGR_ID, mgrServer.getListenPort());
        L.OG("Server started");
        new Thread(mgrServer).start();
    }

    public void receiveResponse(Message response) {

        /* tell the `AsynchronousSystem` the result */
        transactionLock.lock();
        setTransactionResult(response);
        transactionComplete.signalAll();
        transactionLock.unlock();
    }

    public Message getTransactionResult() {
        return transactionResult;
    }

    public void setTransactionResult(Message transactionResult) {
        this.transactionResult = transactionResult;
    }
}
