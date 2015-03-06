package messages;

import messages.vote_req.VoteRequest;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import util.Common;

/**
 * Ethan Petuchowski 3/5/15
 */
public class KillSig extends VoteRequest {

    final int nodeID;

    public KillSig(int nodeID) {
        super(Command.KILL_SIG, Common.NO_ONGOING_TRANSACTION, null);
        this.nodeID = nodeID;
    }

    public int getNodeID() {
        return nodeID;
    }

    @Override protected void writeAsTokens(TokenWriter writer) {
        throw new NotImplementedException();
    }

    @Override protected void readFromTokens(TokenReader reader) {
        throw new NotImplementedException();
    }

    @Override protected String actionLogString() {
        throw new NotImplementedException();
    }
}
