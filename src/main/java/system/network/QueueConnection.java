package system.network;

import messages.Message;

import java.util.Queue;

/**
 * Ethan Petuchowski 2/28/15
 */
public class QueueConnection extends Connection {

    protected Queue<Message> inQueue;
    protected Queue<Message> outQueue;

    public QueueConnection(int receiverID, Queue<Message> inQueue, Queue<Message> outQueue) {
        super(receiverID);
        this.inQueue = inQueue;
        this.outQueue = outQueue;
    }

    @Override public Message receiveMessage() {
        return inQueue.poll();
    }

    @Override public void sendMessage(Message o) {
        outQueue.add(o);
    }

    @Override public boolean isReady() {
        return true;
    }

    public Queue<Message> getInQueue() {
        return inQueue;
    }

    public Queue<Message> getOutQueue() {
        return outQueue;
    }
}
