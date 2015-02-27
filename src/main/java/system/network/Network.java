package system.network;

import system.DistributedSystem;
import system.Message;
import system.RemoteNode;

import java.util.Collection;

/**
 * Ethan Petuchowski 2/17/15
 */
public class Network {

    private NetworkDelay.Type networkDelay;
    private Collection<ObjectConnection> connections;
    private DistributedSystem system;

    public void addConn(ObjectConnection connection) {
        connections.add(connection);
    }

    public int numConnections() {
        return connections.size();
    }

    public Network(NetworkDelay.Type delay, DistributedSystem system) {
        this.networkDelay = delay;
        this.system = system;
    }

    public void send(Message message, RemoteNode to) {

    }

    public void setNetworkDelay(NetworkDelay.Type networkDelay) {
        this.networkDelay = networkDelay;
    }

    public NetworkDelay.Type getNetworkDelay() {
        return networkDelay;
    }

    public void applyConnectivity() {

    }
}
