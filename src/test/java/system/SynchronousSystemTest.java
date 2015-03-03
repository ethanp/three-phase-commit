package system;

import messages.vote_req.AddRequest;
import messages.vote_req.VoteRequest;
import node.PeerReference;
import org.junit.Test;
import util.TestCommon;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

public class SynchronousSystemTest extends TestCommon {

    @Test
    public void testAddRequest() throws Exception {
        final SynchronousSystem system = new SynchronousSystem(3);
        List<PeerReference> peerReferences = system.txnMgr.getNodes()
                                                          .stream()
                                                          .map(mgrNode -> mgrNode.asPeerNode())
                                                          .collect(Collectors.toList());

        VoteRequest voteRequest = new AddRequest(A_SONG_TUPLE, TXID, peerReferences);
        assertTrue(system.handleRequest(voteRequest));

    }
}
