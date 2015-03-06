package system.failures;

import messages.Message;
import messages.TokenReader;
import messages.TokenWriter;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Ethan Petuchowski 3/3/15
 */
public class PartialBroadcast extends Message {

    final int countProcs;
    final int whichProc;
    final Command stage;

    public PartialBroadcast(Command stage, int countProcs, int whichProc) {
        super(Command.PARTIAL_BROADCAST);
        this.stage = stage;
        this.countProcs = countProcs;
        this.whichProc = whichProc;
    }

    @Override protected void writeAsTokens(TokenWriter writer) {
        throw new NotImplementedException();
    }

    @Override protected void readFromTokens(TokenReader reader) {
        throw new NotImplementedException();
    }

    public Command getStage() {
        return stage;
    }

    public int getCountProcs() {
        return countProcs;
    }

    public int getWhichProc() {
        return whichProc;
    }
}
