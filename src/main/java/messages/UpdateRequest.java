package messages;

import util.SongTuple;

/**
 * Ethan Petuchowski 2/27/15
 */
public class UpdateRequest extends Message {
    public UpdateRequest(String songName, SongTuple updatedSong, int msgId) {
        super(Command.UPDATE, msgId);
        this.songName = songName;
        this.updatedSong = updatedSong;
    }
    String songName;
    SongTuple updatedSong;
}
