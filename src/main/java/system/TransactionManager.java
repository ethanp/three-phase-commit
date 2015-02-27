package system;

import messages.Message;
import node.system.SystemNode;
import system.network.Network;
import system.network.NetworkDelay;
import system.network.ObjectConnection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Ethan Petuchowski 2/17/15
 *
 * This is where program execution begins
 */
public class TransactionManager {
    protected List<SystemNodeReference> nodes;
    protected Network network;
    protected SystemServer systemServer;
    protected Failure.Case failureCase;

    public TransactionManager(int numNodes, NetworkDelay.Type delay, Failure.Case aCase) {

        /* start the system management server */
        systemServer = new SystemServer(this);
        new Thread(systemServer).start();

        /* spawn the participant instances, and store references to both their servers and pids */
        nodes = createNodes(numNodes, systemServer.getListenPort());

        network = new Network(delay, this);

        /* wait for everyone to connect to the system (then the callback below gets called) */
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

    public static void main(String[] args) {

        /* for now it only allows the default setup */
        new TransactionManager(
                10,
                NetworkDelay.Type.NONE,
                Failure.Case.NONE);
    }

    public void addConn(ObjectConnection connection) {
        network.addConn(connection);

        try {
            int nodeID = connection.in.readInt();
            int nodeListenPort = connection.in.readInt();
            SystemNodeReference node = remoteNodeWithID(nodeID);
            node.setConn(connection);
            node.setListenPort(nodeListenPort);
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        if (network.numConnections() == nodes.size())
            everyoneConnectedCallback();
    }


    public static List<SystemNodeReference> createNodes(int numNodes, int systemListenPort) {
        final List<String> commandLine = Arrays.asList(
                "java", "-cp", "target/classes", SystemNode.class.getCanonicalName(),
                String.valueOf(systemListenPort), "fakeID"
                                                      );

        /* The start() method creates a new Process instance with those attributes.
           The start() method can be invoked repeatedly from the same instance to
                create new subprocesses with identical or related attributes. */
        final ProcessBuilder procBldr = new ProcessBuilder(commandLine);

        /* merge location of STDOUT and STDERR */
        procBldr.redirectErrorStream(true);

        /* set subprocess STDOUT to the same as the current process */
        procBldr.redirectOutput(ProcessBuilder.Redirect.INHERIT);

        final List<SystemNodeReference> nodes = new ArrayList<>();
        for (int nodeID = 1; nodeID <= numNodes; nodeID++) {
            Process p = null;
            int retries = 0;
            while (p == null && retries++ < 3) {
                try {
                    int lastIdx = procBldr.command().size() - 1;
                    procBldr.command().set(lastIdx, String.valueOf(nodeID));
                    p = procBldr.start();
                    nodes.add(new SystemNodeReference(nodeID, p));
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return nodes;
    }

    SystemNodeReference remoteNodeWithID(int nodeID) {
        for (SystemNodeReference n : nodes)
            if (n.getID() == nodeID)
                return n;
        return null;
    }
}
