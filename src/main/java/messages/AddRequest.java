package messages;

import util.SongTuple;

/**
 * Ethan Petuchowski 2/27/15
 *
 * Sent from TransactionManager to Coordinator
 */
public class AddRequest extends Message {
    public AddRequest(SongTuple songTuple, int transactionID) {
        super(Command.ADD, transactionID);
        this.songTuple = songTuple;
    }

    public SongTuple getSongTuple() {
        return songTuple;
    }

    SongTuple songTuple;
}
