package node.system;

import system.network.MessageReceiver;
import system.network.ObjectConnection;

/**
* Ethan Petuchowski 3/5/15
*/
public class ConnectionListener implements Runnable {
    private MessageReceiver receiver;
    ObjectConnection connection;
    public ConnectionListener(MessageReceiver receiver, ObjectConnection connection) {
        this.receiver = receiver;
        this.connection = connection;
    }

    @Override public void run() {
        while (true) {
            receiver.receiveMessageFrom(connection);
        }
    }
}
