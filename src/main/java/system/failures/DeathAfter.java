package system.failures;

import messages.Message;
import messages.TokenReader;
import messages.TokenWriter;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Ethan Petuchowski 3/3/15
 */
public class DeathAfter extends Message {
    public DeathAfter(int numMsgs, int fromProc, int whichProc) {
        super(Command.DEATH_AFTER);
        this.numMsgs = numMsgs;
        this.fromProc = fromProc;
        this.whichProc = whichProc;
    }

    final int numMsgs;
    final int fromProc;
    final int whichProc;

    public int getNumMsgs() {
        return numMsgs;
    }

    public int getFromProc() {
        return fromProc;
    }

    public int getWhichProc() {
        return whichProc;
    }

    @Override protected void writeAsTokens(TokenWriter writer) {
        throw new NotImplementedException();
    }

    @Override protected void readFromTokens(TokenReader reader) {
        throw new NotImplementedException();
    }
}
