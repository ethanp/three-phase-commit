package messages;

/**
 * Ethan Petuchowski 2/27/15
 */
public class DeleteRequest extends Message {
    public DeleteRequest(String songName, int msgId) {
        super(Message.Command.DELETE, msgId);
        this.songName = songName;
    }

    String songName;

    public String getSongName() {
        return songName;
    }
}
