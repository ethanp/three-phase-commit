package messages.vote_req;

import node.PeerReference;
import util.SongTuple;

import java.util.Collection;

/**
 * Ethan Petuchowski 2/27/15
 */
public class UpdateRequest extends VoteRequest {

    String songName;
    SongTuple updatedSong;

    public UpdateRequest(String songName, SongTuple updatedSong, int msgId, Collection<PeerReference> peerSet) {
        super(Command.UPDATE, msgId, peerSet);
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
}
