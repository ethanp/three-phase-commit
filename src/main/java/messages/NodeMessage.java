package messages;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Ethan Petuchowski 3/3/15
 */
public class NodeMessage extends Message {
    public NodeMessage(int nodeID, int listenPort) {
        super(Command.NODE, -1);
        this.nodeID = nodeID;
        this.listenPort = listenPort;
    }

    final int nodeID;
    final int listenPort;

    @Override protected void writeAsTokens(TokenWriter writer) {
        throw new NotImplementedException();
    }

    @Override protected void readFromTokens(TokenReader reader) {
        throw new NotImplementedException();
    }
}
