package messages.vote_req;

import messages.TokenReader;
import messages.TokenWriter;
import node.PeerReference;
import util.SongTuple;

import java.util.Collection;

/**
 * Ethan Petuchowski 2/27/15
 *
 * Sent from TransactionManager to Coordinator
 */
public class AddRequest extends VoteRequest {
    public AddRequest(SongTuple songTuple, int transactionID, Collection<PeerReference> peerSet) {
        super(Command.ADD, transactionID, peerSet);
        this.songTuple = songTuple;
    }

    public SongTuple getSongTuple() {
        return songTuple;
    }

    SongTuple songTuple;

    @Override protected String actionLogString() {
        return songTuple.toLogString();
    }

	@Override
	protected void writeAsTokens(TokenWriter writer) {
		writer.writeToken(new Integer(transactionID).toString());
		writePeerSetAsTokens(writer);
		songTuple.writeAsTokens(writer);		
	}

	@Override
	protected void readFromTokens(TokenReader reader) {
		transactionID = Integer.parseInt(reader.readToken());
		readPeerSetAsTokens(reader);
		songTuple = SongTuple.readFromTokens(reader);		
	}
}
