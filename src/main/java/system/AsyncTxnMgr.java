package system;

import messages.Message;
import node.system.AsyncProcessNode;
import system.network.ObjectConnection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Ethan Petuchowski 2/28/15
 */
public class AsyncTxnMgr extends TransactionManager {
    public AsyncTxnMgr(int numNodes) {
        super(numNodes);
    }

    protected SystemServer systemServer;

    @Override public List<ManagerNodeRef> createNodes(int numNodes) {
        final List<String> commandLine = Arrays.asList(
                "java", "-cp", "target/classes", AsyncProcessNode.class.getCanonicalName(),
                String.valueOf(systemServer.getListenPort()), "fakeID");

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

    public void addConn(ObjectConnection connection) {
        network.addConn(connection);

        try {
            int nodeID = connection.in.readInt();
            int nodeListenPort = connection.in.readInt();
            ManagerNodeRef node = remoteNodeWithID(nodeID);
            node.setConn(connection);
            node.setListenPort(nodeListenPort);
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        if (network.numConnections() == nodes.size())
            everyoneConnectedCallback();
    }

    /**
     * called by addConn() [below] after we have received connections from all spawned nodes
     */
    void everyoneConnectedCallback() {
        /* wire participants up according to the given delay & connectivity arrangement */
        network.applyConnectivity();

        /* dub someone Coordinator */
        network.send(new Message(Message.Command.DUB_COORDINATOR, 1), nodes.get(0));
    }
}
