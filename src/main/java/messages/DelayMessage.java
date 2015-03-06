package messages;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Ethan Petuchowski 3/6/15
 */
public class DelayMessage extends Message {

    protected final int delaySec;

    public DelayMessage(int delaySeconds) {
        super(Command.DELAY);
        this.delaySec = delaySeconds;
    }

    public int getDelaySec() {
        return delaySec;
    }

    @Override protected void writeAsTokens(TokenWriter writer) {
        throw new NotImplementedException();
    }

    @Override protected void readFromTokens(TokenReader reader) {
        throw new NotImplementedException();
    }
}
