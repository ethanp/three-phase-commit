package system;

import jdk.nashorn.internal.ir.annotations.Ignore;
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

import static messages.Message.Command.COMMIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AsynchronousSystemTest extends TestCommon {
    AsynchronousSystem system;
    List<PeerReference> peerReferences;


    @Before
    public void setUp() throws Exception {
        system = new AsynchronousSystem(2);

        List<ManagerNodeRef> nodes = system.txnMgr.getNodes();

        peerReferences = nodes.stream()
                              .map(mgrNode -> mgrNode.asPeerNode())
                              .collect(Collectors.toList());

        while (system.txnMgr.coordinator == null) {
            system.txnMgr.coordinatorChosen.await();
        }
        System.out.println("JUnit test thinks a coordinator has been chosen");
    }

    @Test
    public void testAddRequest() throws Exception {
        VoteRequest addRequest = new AddRequest(A_SONG_TUPLE, TXID, peerReferences);
        assertEquals(COMMIT, system.processRequestToCompletion(addRequest).getCommand());
    }

    @Test
    public void testParticipantHasUpdatedSongAfterAddAndUpdateRequests() throws Exception {
        VoteRequest add = new AddRequest(A_SONG_TUPLE, TXID, peerReferences);
        assertEquals(COMMIT, system.processRequestToCompletion(add).getCommand());
        VoteRequest up = new UpdateRequest(A_SONG_NAME, SAME_SONG_NEW_URL, TXID+1, peerReferences);
        assertEquals(COMMIT, system.processRequestToCompletion(up).getCommand());

        SyncNode participant = ((SyncManagerNodeRef) system.txnMgr.getNodes().get(1)).getNode();
        assertFalse(participant.hasExactSongTuple(A_SONG_TUPLE));
        assertTrue(participant.hasExactSongTuple(SAME_SONG_NEW_URL));
    }

    @Test
    public void testParticipantDoesNotHaveSongAfterAddAndDeleteRequests() throws Exception {
        VoteRequest addRequest = new AddRequest(A_SONG_TUPLE, TXID, peerReferences);
        assertEquals(COMMIT, system.processRequestToCompletion(addRequest).getCommand());
        VoteRequest deleteRequest = new DeleteRequest(A_SONG_NAME, TXID+1, peerReferences);
        assertEquals(COMMIT, system.processRequestToCompletion(deleteRequest).getCommand());
    }



    /* FAILURE CASES */

    @Ignore
    public void testCoordinatorFailsBeforeSendingVoteReq() throws Exception {

    }

    @Ignore
    public void testParticipantFailsBeforeReceivingVoteReq() throws Exception {

    }

    @Ignore
    public void testCoordinatorFailsAfterSendingAllVoteReqs() throws Exception {

    }

    @Ignore
    public void testCoordinatorFailsAfterSendingSomeVoteReqs() throws Exception {

    }

    @Ignore
    public void testParticipantFailsAfterReceivingVoteReq() throws Exception {

    }

    @Ignore
    public void testParticipantFailsAfterLoggingNOBeforeSendingNO() throws Exception {

    }

    @Ignore
    public void testParticipantFailsAfterLoggingYESBeforeSendingYES() throws Exception {

    }

    @Ignore
    public void testParticipantFailsAfterSendingNO() throws Exception {

    }

    @Ignore
    public void testParticipantFailsAfterSendingYES() throws Exception {

    }

    @Ignore
    public void testCoordinatorFailsAfterReceivingNOBeforeSendingAbort() throws Exception {

    }

    @Ignore
    public void testCoordinatorFailsAfterSendingAbortToEveryone() throws Exception {

    }

    @Ignore
    public void testCoordinatorFailsAfterSendingAbortToOnlySome() throws Exception {

    }

    @Ignore
    public void testCoordinatorFailsAfterReceivingYESesBeforeSendingPrecommit() throws Exception {

    }

    @Ignore
    public void testCoordinatorFailsAfterSendingPrecommitToEveryone() throws Exception {

    }

    @Ignore
    public void testCoordinatorFailsAfterSendingPrecommitToOnlySome() throws Exception {

    }
}
