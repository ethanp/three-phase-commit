package messages;

import util.SongTuple;

/**
 * Ethan Petuchowski 2/27/15
 */
public class UpdateRequest extends Message {

    String songName;
    SongTuple updatedSong;

    public UpdateRequest(String songName, SongTuple updatedSong, int msgId) {
        super(Command.UPDATE, msgId);
        this.songName = songName;
        this.updatedSong = updatedSong;
    }

    public String getSongName() {
        return songName;
    }

    public SongTuple getUpdatedSong() {
        return updatedSong;
    }
}
