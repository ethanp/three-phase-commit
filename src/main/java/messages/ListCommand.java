package messages;

import messages.vote_req.VoteRequest;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import util.Common;

/**
 * Ethan Petuchowski 3/8/15
 */
public class ListCommand extends VoteRequest {

    public ListCommand() {
        super(Command.LIST, Common.INVALID_ID, null);
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
