package system.network;

/**
 * Ethan Petuchowski 3/5/15
 */
public interface MessageReceiver {
    boolean receiveMessageFrom(Connection connection, int msgsRcvd);
}
