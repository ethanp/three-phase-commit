package system;

import messages.vote_req.AddRequest;
import messages.vote_req.DeleteRequest;
import messages.vote_req.UpdateRequest;
import messages.vote_req.VoteRequest;
import node.PeerReference;
import node.system.SyncNode;
import org.junit.Before;
import org.junit.Test;
import util.TestCommon;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AsynchronousSystemTest extends TestCommon {
    AsynchronousSystem system;
    List<PeerReference> peerReferences;

    @Before
    public void setUp() throws Exception {
        system = new AsynchronousSystem(5);

        List<ManagerNodeRef> nodes = system.txnMgr.getNodes();

        peerReferences = nodes.stream()
                              .map(mgrNode -> mgrNode.asPeerNode())
                              .collect(Collectors.toList());
    }

    @Test
    public void testAddRequest() throws Exception {
        VoteRequest addRequest = new AddRequest(A_SONG_TUPLE, TXID, peerReferences);
        assertTrue(system.processRequestToCompletion(addRequest));
    }

    @Test
    public void testParticipantHasUpdatedSongAfterAddAndUpdateRequests() throws Exception {
        VoteRequest add = new AddRequest(A_SONG_TUPLE, TXID, peerReferences);
        assertTrue(system.processRequestToCompletion(add));
        VoteRequest up = new UpdateRequest(A_SONG_NAME, SAME_SONG_NEW_URL, TXID+1, peerReferences);
        assertTrue(system.processRequestToCompletion(up));

        SyncNode participant = ((SyncManagerNodeRef) system.txnMgr.getNodes().get(1)).getNode();
        assertFalse(participant.hasExactSongTuple(A_SONG_TUPLE));
        assertTrue(participant.hasExactSongTuple(SAME_SONG_NEW_URL));
    }

    @Test
    public void testParticipantDoesNotHaveSongAfterAddAndDeleteRequests() throws Exception {
        VoteRequest addRequest = new AddRequest(A_SONG_TUPLE, TXID, peerReferences);
        assertTrue(system.processRequestToCompletion(addRequest));
        VoteRequest deleteRequest = new DeleteRequest(A_SONG_NAME, TXID+1, peerReferences);
        assertTrue(system.processRequestToCompletion(deleteRequest));
    }
}
