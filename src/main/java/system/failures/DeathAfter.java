package system.failures;

import messages.Message;
import messages.TokenReader;
import messages.TokenWriter;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Ethan Petuchowski 3/3/15
 */
public class DeathAfter extends Message {
    public DeathAfter(int numMsgs, int procID) {
        super(Command.DEATH_AFTER);
        this.numMsgs = numMsgs;
        this.procID = procID;
    }

    final int numMsgs;
    final int procID;

    public int getNumMsgs() {
        return numMsgs;
    }

    public int getProcID() {
        return procID;
    }

    @Override protected void writeAsTokens(TokenWriter writer) {
        throw new NotImplementedException();
    }

    @Override protected void readFromTokens(TokenReader reader) {
        throw new NotImplementedException();
    }
}
