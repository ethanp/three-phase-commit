package messages.vote_req;

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
}
