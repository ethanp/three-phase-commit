package system;

import console.ConsoleCommand;
import jdk.nashorn.internal.ir.annotations.Ignore;
import messages.Message;
import messages.vote_req.AddRequest;
import messages.vote_req.DeleteRequest;
import messages.vote_req.UpdateRequest;
import messages.vote_req.VoteRequest;
import node.PeerReference;
import node.system.FileDTLog;
import org.junit.Before;
import org.junit.Test;
import util.Common;
import util.TestCommon;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static messages.Message.Command.ABORT;
import static messages.Message.Command.COMMIT;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

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
        while (system.txnMgr.getCoordinator() == null) {
            system.txnMgr.coordinatorChosen.await();
        }
        System.out.println("JUnit test thinks a coordinator has been chosen");
    }

    @Test
    public void testAddRequest() throws Exception {
        VoteRequest addRequest = new AddRequest(A_SONG_TUPLE, TXID, peerReferences);
        assertEquals(COMMIT, system.processRequestToCompletion(addRequest).getCommand());

        Thread.sleep(Common.TIMEOUT_MILLISECONDS());
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

        Thread.sleep(Common.TIMEOUT_MILLISECONDS());
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

        Thread.sleep(Common.TIMEOUT_MILLISECONDS());
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

    /**
     * according to https://piazza.com/class/i5h1h4rqk9t4si?cid=47 we can ignore this case.
     */
    @Ignore public void testCoordinatorFailsBeforeSendingVoteReq() throws Exception {}

    @Test
    public void testParticipantFailsBeforeReceivingVoteReq() throws Exception {
      /*
            TxnMgr
                creates nodes
                dubs 1 coordinator

            Coordinator
                receives add request with 2's listen port
                connects to node 2, and sends vital stats

            Node 2
                saves Coord's vital stats
                listens for messages
                CRASHES.

            Coordinator
                sends ADD to 2
                times-out on 2
                tells TxnMgr 2 is dead
                ABORTS transaction

            TxnMgr
                receives "2 is dead" from Coord
                restarts 2
                receives ABORT, relays to System

            Node 2
                revives from it's log: nothing logged

        */
        String cmdStr = "add a_song a_url -deathAfter 0 1 2";
        ConsoleCommand command = new ConsoleCommand(cmdStr, TXID);
        assertEquals(ABORT, system.processCommandToCompletion(command).getCommand());

        Thread.sleep(Common.TIMEOUT_MILLISECONDS());
        system.killAllNodes();
    }

    @Test
    public void testParticipantFailsAfterSendingNO_txnAborts() throws Exception {
        /* this is already implementable

            Start up the system (happens in @Before)
            Put a COMMIT of the song in the log of node "2"
            Have the system "restart" node 2
                Now it ought to read the commit from the log on startup

            [2 2015-03-06T22:46:43.773Z] ADD  1  5  1  3006  2  3002  3  3001  4  3003  5  3004  a_song  a_url
            [2 2015-03-06T22:46:43.780Z] YES  1
            [2 2015-03-06T22:46:44.797Z] COMMIT  1

        */

        final File logFile2 = new File("logDir", "2");
        PrintWriter p = new PrintWriter(new FileWriter(logFile2));
        p.println("[2 2015-03-06T22:46:43.773Z] ADD  1  5  1  3006  2  3002  3  3001  4  3003  5  3004  a_song  a_url");
        p.println("[2 2015-03-06T22:46:43.780Z] YES  1");
        p.println("[2 2015-03-06T22:46:44.797Z] COMMIT  1");
        p.flush();

        system.getTxnMgr().restartNodeWithID(2);
        Thread.sleep(Common.TIMEOUT_MILLISECONDS());

        String cmdStr = "add a_song a_url -deathAfter 1 1 2";
        ConsoleCommand command = new ConsoleCommand(cmdStr, TXID);
        assertEquals(ABORT, system.processCommandToCompletion(command).getCommand());

        Thread.sleep(Common.TIMEOUT_MILLISECONDS()*2);
        system.killAllNodes();
    }

    @Test
    public void testParticipantFailsAfterSendingYES() throws Exception {

        String cmdStr = "add a_song a_url -deathAfter 2 1 2";
        ConsoleCommand command = new ConsoleCommand(cmdStr, TXID);
        assertEquals(COMMIT, system.processCommandToCompletion(command).getCommand());

        Thread.sleep(Common.TIMEOUT_MILLISECONDS()*2);
        assertLogContains(2, COMMIT, TXID);
        system.killAllNodes();
    }

    @Test
    public void testCoordinatorFailsAfterSendingPrecommitToTwo() throws Exception {

        /* what SHOULD happen

            TxnMgr
                sends AddReq to Coord
            C1
                broadcasts AddReq to all
            P2-5
                send YES to Coord
            C1
                sends PRE_CMT to 2,3
                fails
            P4,5
                Timeout on response from Coord
                send UR_EL to 2
            P2
                receives UR_EL
                becomes Coordinator in Term-Protocol
            C2
                sends STATE_REQ to 3-5
            P3
                receive STATE_REQ
                sends PRE_CMT
            P4-5
                receive STATE_REQ
                send Uncertain
            C2
                send PRE_CMT to 4-5
                wait for ACKS
            P4-5
                receive PRE_CMT
                send ACK
            C2
                receive ACKS
                broadcast COMMIT
            R1
                send DEC_REQ to 2
            C2
                sends COMMIT to 1

            We need to adjust P->C so that C gets whether P was precommitted
         */
        runAndCheckCommandCommitted("add a_song a_url -partialPrecommit 2 1", 6);
    }

    @Test
    public void testCoordinatorFailsPrecommitToAll() throws Exception {
        runAndCheckCommandCommitted("add a_song a_url -partialPrecommit 4 1", 2);
    }

    @Test
    public void testCoordinatorFailsCommitToTwo() throws Exception {
        runAndCheckCommandCommitted("add a_song a_url -partialCommit 2 1", 4);
    }

    @Test
    public void testCoordinatorFailsCommitToAll() throws Exception {
        runAndCheckCommandCommitted("add a_song a_url -partialCommit 4 1", 2);
    }

    @Test
    public void testFutureCoordinatorFailure() throws Exception {
        runAndCheckCommandCommitted("add a_song a_url -deathAfter 1 3 2 -deathAfter 1 4 2 -deathAfter 1 5 2 -partialPrecommit 3 1", 4);
    }

    private void assertLogContains(int nodeID, Message.Command command, int txid) throws IOException {
        Collection<Message> msgs = new FileDTLog(new File("logDir", String.valueOf(nodeID)), null).getLoggedMessages();
        Optional<Message> opM = msgs.stream()
                                    .filter(m -> m.getCommand() == command && m.getTransactionID() == txid)
                                    .findFirst();
        assertTrue("Node "+nodeID+" doesn't have "+command.toString(), opM.isPresent());
    }

    private void runAndCheckCommandCommitted(String cmdStr, int timeoutMultiplier) throws Exception {
        runAndCheckCommand(cmdStr, timeoutMultiplier, COMMIT);
    }

    private void runAndCheckCommand(String cmdStr, int timeoutMultiplier, Message.Command result) throws InterruptedException, IOException {
        ConsoleCommand command = new ConsoleCommand(cmdStr, system.getTxnMgr().getNextTransactionID());
        assertEquals(result, system.processCommandToCompletion(command).getCommand());

        Thread.sleep(Common.TIMEOUT_MILLISECONDS()*timeoutMultiplier);
        for (int i = 1; i <= 5; i++) {
            assertLogContains(i, result, TXID);
        }
        system.killAllNodes();
    }

    @Test
    public void testCascadingCoordinatorFailure_5staysAlive() throws Exception {
        runAndCheckCommand("add a b "+
                           "-partialPrecommit 2 1 "+
                           "-deathAfterElected 2 "+
                           "-deathAfterElected 3 "+
                           "-deathAfterElected 4",
                           4, ABORT);
    }

    @Test
    public void testCascadingCoordinatorFailure_allDie() throws Exception {
        runAndCheckCommand("add a b "+
                           "-partialPrecommit 2 1 "+
                           "-deathAfterElected 2 "+
                           "-deathAfterElected 3 "+
                           "-deathAfterElected 4 "+
                           "-deathAfterElected 5",
                           8, ABORT);
    }
}
