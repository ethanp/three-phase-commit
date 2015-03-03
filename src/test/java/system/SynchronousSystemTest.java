package system;

import messages.vote_req.AddRequest;
import messages.vote_req.DeleteRequest;
import messages.vote_req.UpdateRequest;
import messages.vote_req.VoteRequest;
import node.PeerReference;
import node.system.SyncNode;
import org.junit.Before;
import org.junit.Test;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import util.TestCommon;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SynchronousSystemTest extends TestCommon {

    SyncNode coordinator;
    SyncNode participant;
    SynchronousSystem system;
    List<PeerReference> peerReferences;

    @Before
    public void setUp() throws Exception {
        system = new SynchronousSystem(5);

        List<ManagerNodeRef> nodes = system.txnMgr.getNodes();

        peerReferences = nodes.stream()
                              .map(mgrNode -> mgrNode.asPeerNode())
                              .collect(Collectors.toList());

        coordinator = ((SyncManagerNodeRef) nodes.get(0)).getNode();
        participant = ((SyncManagerNodeRef) nodes.get(1)).getNode();
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

    @Test
    public void testReceiveAbortWhenUpdatingNonexistentSong() throws Exception {
        throw new NotImplementedException();
    }

    @Test
    public void testReceiveAbortWhenAddingExistingSong() throws Exception {
        throw new NotImplementedException();
    }

    @Test
    public void testReceiveAbortWhenDeletingNonexistentSong() throws Exception {
        throw new NotImplementedException();
    }

    @Test
    public void testReceiveAbortWhenAddingPartiallyReplicatedSong_onlyOnAParticipant() throws Exception {
        throw new NotImplementedException();
    }

    @Test
    public void testReceiveAbortWhenAddingPartiallyReplicatedSong_onlyOnCoordinator() throws Exception {
        throw new NotImplementedException();
    }

    @Test
    public void testReceiveAbortWhenUpdatingPartiallyReplicatedSong_onlyOnAParticipant() throws Exception {
        throw new NotImplementedException();
    }

    @Test
    public void testReceiveAbortWhenUpdatingPartiallyReplicatedSong_onlyOnCoordinator() throws Exception {
        throw new NotImplementedException();
    }

    @Test
    public void testReceiveAbortWhenDeletingPartiallyReplicatedSong_onlyAParticipantIsMissingIt() throws Exception {
        throw new NotImplementedException();
    }

    @Test
    public void testReceiveAbortWhenDeletingPartiallyReplicatedSong_onlyCoordinatorIsMissingIt() throws Exception {
        throw new NotImplementedException();
    }
}
