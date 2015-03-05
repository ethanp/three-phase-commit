package util;

import messages.Message;
import node.PeerReference;

import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Queue;

/**
 * Ethan Petuchowski 2/28/15
 */
public class TestCommon {
    public final static int TXID = 1;
    public final static Collection<PeerReference> A_PEER_REFS = new ArrayList<>(Arrays.asList(
            new PeerReference(1, 23),
            new PeerReference(2, 34),
            new PeerReference(3, 45)));
    public final static String A_SONG_NAME = "sample name"; // should work even
    public final static String A_URL = "sample url";        // if they have spaces
    public final static String DIFFERENT_URL = "a different url";
    public final static SongTuple A_SONG_TUPLE = new SongTuple(A_SONG_NAME, A_URL);
    public final static SongTuple SAME_SONG_NEW_URL = new SongTuple(A_SONG_NAME, DIFFERENT_URL);
    public static final int TEST_PEER_ID = 2;
    public static final int TEST_COORD_ID = 1;
    @Rule public ExpectedException exception = ExpectedException.none();    

    protected Message getLastMessageInQueue(Queue<Message> queue) {
    	Message message;
    	do {
    		message = queue.poll();
    	} while (queue.size() > 0);
    	return message;    	
    }
}
