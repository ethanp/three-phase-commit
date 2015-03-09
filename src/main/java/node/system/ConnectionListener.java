package node.system;

import system.network.MessageReceiver;
import system.network.ObjectConnection;

/**
* Ethan Petuchowski 3/5/15
*/
public class ConnectionListener implements Runnable {
    private MessageReceiver receiver;
    ObjectConnection connection;
    int msgsRcvd = 0;
    public ConnectionListener(MessageReceiver receiver, ObjectConnection connection) {
        this.receiver = receiver;
        this.connection = connection;
    }

    @Override public void run() {
        while (connection.isReady()) {
            receiver.receiveMessageFrom(connection, ++msgsRcvd);
        }
    }
}
