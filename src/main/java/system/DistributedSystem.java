package system;

import system.network.Network;
import system.network.NetworkDelay;
import system.network.ObjectConnection;
import system.node.Node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Ethan Petuchowski 2/17/15
 *
 * This is where program execution begins
 */
public class DistributedSystem {
    protected List<RemoteNode> nodes;
    protected Network network;
    protected SystemServer systemServer;
    protected Failure.Model failureModel;

    public DistributedSystem(int numNodes, Network.Connectivity connectivity, NetworkDelay.Type delay, Failure.Model model) {

        /* start the system management server */
        systemServer = new SystemServer(this);
        new Thread(systemServer).start();

        /* spawn the participant instances, and store references to both their servers and pids */
        nodes = createNodes(numNodes, systemServer.getListenPort());

        network = new Network(delay, connectivity, this);

        /* wait for everyone to connect to the system (then the callback below gets called) */
    }

    /**
     * called by addConn() [below] after we have received connections from all spawned nodes
     */
    void everyoneConnectedCallback() {
        /* wire participants up according to the given delay & connectivity arrangement */
        network.applyConnectivity();

        /* TODO dub someone Coordinator */
        /* TODO initiate protocol according to failure model ? */
    }

    public static void main(String[] args) {

        /* for now it only allows the default setup */
        new DistributedSystem(
                3,
                Network.Connectivity.ALL_TO_ALL,
                NetworkDelay.Type.NONE,
                Failure.Model.NONE);
    }

    public void addConn(ObjectConnection connection) {
        network.addConn(connection);

        try {
            int nodeID = connection.in.readInt();
            int nodeListenPort = connection.in.readInt();
            RemoteNode node = remoteNodeWithID(nodeID);
            node.setConn(connection);
            node.setListenPort(nodeListenPort);
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        if (network.numConnections() == nodes.size())
            everyoneConnectedCallback();
    }


    public static List<RemoteNode> createNodes(int numNodes, int systemListenPort) {
        final List<String> commandLine = Arrays.asList(
                "java", "-cp", "target/classes", Node.class.getCanonicalName(),
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

        final List<RemoteNode> nodes = new ArrayList<>();
        for (int nodeID = 1; nodeID <= numNodes; nodeID++) {
            Process p = null;
            int retries = 0;
            while (p == null && retries++ < 3) {
                try {
                    int lastIdx = procBldr.command().size() - 1;
                    procBldr.command().set(lastIdx, String.valueOf(nodeID));
                    p = procBldr.start();
                    nodes.add(new RemoteNode(nodeID, p));
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return nodes;
    }

    RemoteNode remoteNodeWithID(int nodeID) {
        for (RemoteNode n : nodes)
            if (n.getID() == nodeID)
                return n;
        return null;
    }
}
