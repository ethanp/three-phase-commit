package messages.vote_req;

import messages.Message;
import node.PeerReference;

import java.util.Collection;

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
}
