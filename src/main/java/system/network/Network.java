package system.network;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import system.DistributedSystem;
import system.Message;
import system.node.Node;

import java.util.Collection;

/**
 * Ethan Petuchowski 2/17/15
 */
public class Network {

    private NetworkDelay.Type networkDelay;
    private Connectivity connectivity;
    private Collection<ObjectConnection> connections;
    private DistributedSystem system;

    public void addConn(ObjectConnection connection) {
        connections.add(connection);
    }

    public int numConnections() {
        return connections.size();
    }

    public enum Connectivity {
        ALL_TO_ALL
    }

    public Network(NetworkDelay.Type delay, Connectivity connectivity, DistributedSystem system) {
        this.networkDelay = delay;
        this.connectivity = connectivity;
        this.system = system;
    }

    public void send(Message message, Node from, Node to) {

    }

    public void setNetworkDelay(NetworkDelay.Type networkDelay) {
        this.networkDelay = networkDelay;
    }

    public NetworkDelay.Type getNetworkDelay() {
        return networkDelay;
    }

    public void setConnectivity(Connectivity connectivity) {
        this.connectivity = connectivity;
        switch (connectivity) {
            case ALL_TO_ALL:
                break;
            default:
                throw new NotImplementedException();
        }
    }

    public Connectivity getConnectivity() {
        return connectivity;
    }

    public void applyConnectivity() {

    }
}
