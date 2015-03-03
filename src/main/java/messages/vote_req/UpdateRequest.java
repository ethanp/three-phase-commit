package messages.vote_req;

import messages.TokenReader;
import messages.TokenWriter;
import node.PeerReference;
import util.SongTuple;

import java.util.Collection;

/**
 * Ethan Petuchowski 2/27/15
 *
 * "change the name and/or url of a given song tuple"
 */
public class UpdateRequest extends VoteRequest {

    String songName;
    SongTuple updatedSong;

    public UpdateRequest(String songName, SongTuple updatedSong, int transactionID, Collection<PeerReference> peerSet) {
        super(Command.UPDATE, transactionID, peerSet);
        this.songName = songName;
        this.updatedSong = updatedSong;
    }

    public String getSongName() {
        return songName;
    }

    public SongTuple getUpdatedSong() {
        return updatedSong;
    }

    @Override protected String actionLogString() {
        return songName+"\n"+updatedSong.toLogString();
    }

	@Override
	protected void writeAsTokens(TokenWriter writer) {
		writer.writeToken(new Integer(transactionID).toString());
		writePeerSetAsTokens(writer);
		writer.writeToken(songName);		
		updatedSong.writeAsTokens(writer);
	}

	@Override
	protected void readFromTokens(TokenReader reader) {
		transactionID = Integer.parseInt(reader.readToken());
		readPeerSetAsTokens(reader);
		songName = reader.readToken();
		updatedSong = SongTuple.readFromTokens(reader);
	}
}
