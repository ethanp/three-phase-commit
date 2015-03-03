package system.network;

import messages.Message;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import system.ManagerNodeRef;
import system.TransactionManager;

import java.util.Collection;

/**
 * Ethan Petuchowski 2/17/15
 */
public class Network {

    private NetworkDelay.Type networkDelay;
    private Collection<ObjectConnection> connections;
    private TransactionManager system;

    public void addConn(ObjectConnection connection) {
        connections.add(connection);
    }

    public int numConnections() {
        return connections.size();
    }

    public Network(NetworkDelay.Type delay, TransactionManager system) {
        this.networkDelay = delay;
        this.system = system;
    }

    public void send(Message message, ManagerNodeRef to) {
        throw new NotImplementedException();
    }

    public void setNetworkDelay(NetworkDelay.Type networkDelay) {
        this.networkDelay = networkDelay;
    }

    public NetworkDelay.Type getNetworkDelay() {
        return networkDelay;
    }

    public void applyConnectivity() {
        throw new NotImplementedException();
    }
}
