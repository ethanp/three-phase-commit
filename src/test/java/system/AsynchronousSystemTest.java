package system;

import jdk.nashorn.internal.ir.annotations.Ignore;
import messages.vote_req.AddRequest;
import messages.vote_req.DeleteRequest;
import messages.vote_req.UpdateRequest;
import messages.vote_req.VoteRequest;
import node.PeerReference;
import org.junit.Before;
import org.junit.Test;
import system.failures.DeathAfter;
import system.failures.Failure;
import util.Common;
import util.TestCommon;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;

import static messages.Message.Command.COMMIT;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class AsynchronousSystemTest extends TestCommon {
    AsynchronousSystem system;
    List<PeerReference> peerReferences;

    String coordLogString() { return readLogNum(1); }
    String particLogString() { return readLogNum(2); }

    String readLogNum(int nodeID) {
        try {
            return new String(
                    Files.readAllBytes(
                            new File(Common.LOG_DIR, String.valueOf(nodeID)).toPath()));
        }
        catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Before
    public void setUp() throws Exception {
        system = new AsynchronousSystem(5);
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

        Thread.sleep(Common.TIMEOUT_MILLISECONDS);
        system.killAllNodes();

        String coordLog = coordLogString();
        String particLog = particLogString();

        assertThat(coordLog, containsString("ADD  1"));
        assertThat(coordLog, containsString("COMMIT  1"));
        assertThat(particLog, containsString("ADD  1"));
        assertThat(particLog, containsString("COMMIT  1"));
    }

    @Test
    public void testAddThenUpdateRequestsCommit() throws Exception {
        VoteRequest add = new AddRequest(A_SONG_TUPLE, TXID, peerReferences);
        assertEquals(COMMIT, system.processRequestToCompletion(add).getCommand());
        VoteRequest up = new UpdateRequest(A_SONG_NAME, SAME_SONG_NEW_URL, TXID+1, peerReferences);
        assertEquals(COMMIT, system.processRequestToCompletion(up).getCommand());

        Thread.sleep(Common.TIMEOUT_MILLISECONDS);
        system.killAllNodes();

        String coordLog = coordLogString();
        String particLog = particLogString();

        assertThat(coordLog, containsString("ADD  1"));
        assertThat(coordLog, containsString("COMMIT  1"));
        assertThat(coordLog, containsString("UPDATE  2"));
        assertThat(coordLog, containsString("COMMIT  2"));

        assertThat(particLog, containsString("ADD  1"));
        assertThat(particLog, containsString("COMMIT  1"));
        assertThat(particLog, containsString("UPDATE  2"));
        assertThat(particLog, containsString("COMMIT  2"));
    }

    @Test
    public void testAddThenDeleteRequestsCommit() throws Exception {
        VoteRequest addRequest = new AddRequest(A_SONG_TUPLE, TXID, peerReferences);
        assertEquals(COMMIT, system.processRequestToCompletion(addRequest).getCommand());
        VoteRequest deleteRequest = new DeleteRequest(A_SONG_NAME, TXID+1, peerReferences);
        assertEquals(COMMIT, system.processRequestToCompletion(deleteRequest).getCommand());

        Thread.sleep(Common.TIMEOUT_MILLISECONDS);
        system.killAllNodes();

        String coordLog = coordLogString();
        String particLog = particLogString();

        assertThat(coordLog, containsString("ADD  1"));
        assertThat(coordLog, containsString("COMMIT  1"));
        assertThat(coordLog, containsString("DELETE  2"));
        assertThat(coordLog, containsString("COMMIT  2"));

        assertThat(particLog, containsString("ADD  1"));
        assertThat(particLog, containsString("COMMIT  1"));
        assertThat(particLog, containsString("DELETE  2"));
        assertThat(particLog, containsString("COMMIT  2"));
    }


    /* FAILURE CASES */

    @Ignore
    public void testCoordinatorFailsBeforeSendingVoteReq() throws Exception {
        Failure f = new DeathAfter(TestCommon.TEST_COORD_ID, 0);
        VoteRequest addRequest = new AddRequest(A_SONG_TUPLE, TXID, peerReferences);
        assertEquals(COMMIT, system.processRequestToCompletion(addRequest).getCommand());
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
