package messages.vote_req;

import messages.Message;
import messages.TokenReader;
import messages.TokenWriter;
import node.PeerReference;

import java.util.Collection;

import util.SongTuple;

/**
 * Ethan Petuchowski 2/27/15
 */
public class DeleteRequest extends VoteRequest {
    public DeleteRequest(String songName, int transactionID, Collection<PeerReference> peerSet) {
        super(Message.Command.DELETE, transactionID, peerSet);
        this.songName = songName;
    }

    String songName;

    public String getSongName() {
        return songName;
    }

    @Override protected String actionLogString() {
        return songName;
    }

	@Override
	protected void writeAsTokens(TokenWriter writer) {
		writer.writeToken(new Integer(transactionID).toString());		
		writePeerSetAsTokens(writer);
		writer.writeToken(songName);		
	}

	@Override
	protected void readFromTokens(TokenReader reader) {
		transactionID = Integer.parseInt(reader.readToken());
		readPeerSetAsTokens(reader);
		songName = reader.readToken();		
	}
}
