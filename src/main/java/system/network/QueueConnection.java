package system.network;

import messages.Message;

import java.util.Queue;

/**
 * Ethan Petuchowski 2/28/15
 */
public class QueueConnection extends Connection {

    public QueueConnection(int receiverID) {
        super(receiverID);
    }

    Queue<Message> inQueue;
    Queue<Message> outQueue;

    public QueueConnection(int receiverID, Queue<Message> inQueue, Queue<Message> outQueue) {
        super(receiverID);
        this.inQueue = inQueue;
        this.outQueue = outQueue;
    }

    @Override public Message readObject() {
        return inQueue.poll();
    }

    @Override public void writeObject(Message o) {
        outQueue.add(o);
    }
}
