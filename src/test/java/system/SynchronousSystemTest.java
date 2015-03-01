package system;

import messages.vote_req.AddRequest;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static util.TestCommon.A_PEER_REFS;
import static util.TestCommon.A_SONG_TUPLE;
import static util.TestCommon.TXID;

public class SynchronousSystemTest {
    @Test
    public void testAddRequest() throws Exception {
        AddRequest addRequest = new AddRequest(A_SONG_TUPLE, TXID, A_PEER_REFS);
        final SynchronousSystem system = new SynchronousSystem(addRequest);
        assertTrue(system.getResult());
    }
}
