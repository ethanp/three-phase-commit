package util;

import node.PeerReference;

import java.util.Arrays;
import java.util.Collection;

/**
 * Ethan Petuchowski 2/28/15
 */
public class TestCommon {
    public final static int TXID = 1;
    public final static Collection<PeerReference> A_PEER_REFS = Arrays.asList(
            new PeerReference(2, 2),
            new PeerReference(3, 3),
            new PeerReference(4, 4));
    public final static String A_SONG_NAME = "name";
    public final static String A_URL = "url";
    public final static SongTuple A_SONG_TUPLE = new SongTuple(A_SONG_NAME, A_URL);
}
