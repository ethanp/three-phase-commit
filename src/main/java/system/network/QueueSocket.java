package system.network;

import messages.Message;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Ethan Petuchowski 2/28/15
 */
public class QueueSocket {
    Queue<Message> aSideQueue = new ArrayDeque<>();
    Queue<Message> bSideQueue = new ArrayDeque<>();
    int aId, bId;
    public QueueSocket(int aId, int bId) {
        this.aId = aId;
        this.bId = bId;
    }
    public QueueConnection getConnectionToAID() {
        return new QueueConnection(aId, aSideQueue, bSideQueue);
    }

    public QueueConnection getConnectionToBID() {
        return new QueueConnection(bId, bSideQueue, aSideQueue);
    }
}
