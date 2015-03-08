package system;

import console.CommandConsole;
import console.ConsoleCommand;
import messages.DecisionRequest;
import messages.DelayMessage;
import messages.KillSig;
import messages.Message;
import messages.PeerTimeout;
import node.system.AsyncLogger;
import node.system.AsyncProcessNode;
import system.failures.DeathAfter;
import system.failures.PartialBroadcast;
import util.Common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

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
        console = new CommandConsole(this);
        new Thread(console).start();
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

    protected int transactionID = 0;

    protected CommandConsole console;


    long getNumConnectedNodes() {
        return nodes.stream().filter(n -> n.getConn() != null).count();
    }

    public void processCommand(ConsoleCommand command) {

        transactionResult = null;
        /**
         * Actually INSTEAD of passing the failures to the node as a commandline param
         * we should send it to the node as a message
         * so that we don't have to kill and restart them.
         */
        for (Message failure : command.getFailureModes()) {
            if (failure instanceof PartialBroadcast) {
                send(((PartialBroadcast) failure).getWhichProc(), failure);
            }
            /**
             * NB: You can only send a node ONE DeathAfter for it to know until it fails.
             * This would be simple-ish to fix though.
             */
            else if (failure instanceof DeathAfter) {
                int procID = ((DeathAfter) failure).getWhichProc();
                send(procID, failure);
            }
        }

        if (command.getDelay() > -1) {
            broadcast(new DelayMessage(command.getDelay()));
        }

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
                addPeerSet(command);
                processRequest(command.getVoteRequest());
        }

    }

    /**
     * add all the nodes the txn-mgr knows about to the peer-set attached to the vote-request
     */
    private void addPeerSet(ConsoleCommand command) {
        command.getVoteRequest().setPeerSet(
                getNodes().stream()
                          .map(ManagerNodeRef::asPeerNode)
                          .collect(Collectors.toList()));
    }

    private void restartNode(ManagerNodeRef nodeToKill) {
        nodeToKill.killNode();
        createNode(nodeToKill.getNodeID());
    }

    public void restartNodeWithID(int nodeID) {
        System.out.println("Mgr restarting node "+nodeID);
        restartNode(getNodeByID(nodeID));
    }

    public ManagerNodeRef createNode(int nodeID) {

        final List<String> commandLine = Arrays.asList(
                "java", "-cp", "target/classes", AsyncProcessNode.class.getCanonicalName(),
                String.valueOf(nodeID),
                String.valueOf(mgrServer.getListenPort()));

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
        new Thread(mgrServer).start();
    }

    public synchronized void receiveResponse(Message response) {
        switch (response.getCommand()) {
            case COMMIT:
            case ABORT:
                signalSystem(response);
                break;

            case TIMEOUT:
                final int peerId = ((PeerTimeout) response).getPeerId();
                if (peerId > getCoordinator().getNodeID()) {
                    reviveNode(peerId);
                }
                break;

            default:
                if (getTransactionResult() == null) {
                    try { Thread.sleep(500); } catch (InterruptedException e) {}
                    sendCoordinator(new DecisionRequest(getTransactionID()));
                }
                break;
        }
    }

    protected void reviveNode(int deadID) {
        if (((AsyncManagerNodeRef)getNodeByID(deadID)).isAlive()) {
            return;
        }
        nodes = getNodes().stream()
                          .filter(r -> r.getNodeID() != deadID)
                          .collect(Collectors.toList());
        L.OG("Reviving node "+deadID);
        final ManagerNodeRef newNode = createNode(deadID);
        nodes.add(newNode);
        if (deadID == 1) {
            setCoordinator(newNode);
        }
    }

    private void signalSystem(Message response) {
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

    public int getTransactionID() {
        return transactionID;
    }

    public int getNextTransactionID() {
        return ++transactionID;
    }
}
