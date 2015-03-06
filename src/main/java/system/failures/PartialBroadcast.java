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
    final Command stage;

    public PartialBroadcast(Command stage, int countProcs) {
        super(Command.PARTIAL_BROADCAST);
        this.stage = stage;
        this.countProcs = countProcs;
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
}
