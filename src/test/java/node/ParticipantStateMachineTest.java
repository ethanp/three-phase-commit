package node;

import messages.CommitRequest;
import messages.Message;
import messages.PrecommitRequest;
import messages.vote_req.AddRequest;
import messages.vote_req.DeleteRequest;
import messages.vote_req.UpdateRequest;
import messages.vote_req.VoteRequest;
import node.system.SyncNode;
import org.junit.Before;
import org.junit.Test;
import system.network.QueueConnection;
import system.network.QueueSocket;
import util.SongTuple;
import util.TestCommon;

import java.util.PriorityQueue;
import java.util.stream.Collectors;

import static messages.Message.Command.ABORT;
import static messages.Message.Command.ACK;
import static messages.Message.Command.DUB_COORDINATOR;
import static messages.Message.Command.UR_ELECTED;
import static messages.Message.Command.YES;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static util.Common.NO_ONGOING_TRANSACTION;

public class ParticipantStateMachineTest extends TestCommon {

    SyncNode participantUnderTest;
    ParticipantStateMachine participantSM;
    QueueSocket queueSocket;
    QueueConnection peerToCoordinator;
    QueueConnection coordinatorToPeer;

    @Before
    public void setUp() throws Exception {
        participantUnderTest = new SyncNode(TEST_PEER_ID, null);
        participantSM = (ParticipantStateMachine) participantUnderTest.getStateMachine();
        queueSocket = new QueueSocket(TEST_COORD_ID, TEST_PEER_ID);
        peerToCoordinator = queueSocket.getConnectionToAID();
        participantUnderTest.addConnection(peerToCoordinator);
        coordinatorToPeer = queueSocket.getConnectionToBID();
    }

    private void testReceiveFromCoordinator(Message message) {
        coordinatorToPeer.sendMessage(message);
        assertTrue(participantSM.receiveMessage(peerToCoordinator));
    }

    private void assertStateAfterNOSentToCoordinator() {
        assertThat(participantUnderTest.getDtLog().getLogAsString(), containsString("ABORT"));
        /* NO was sent to coordinator */
        assertEquals(1, peerToCoordinator.getOutQueue().size());
        Message response = coordinatorToPeer.receiveMessage();
        assertTrue(Message.Command.NO.equals(response.getCommand()));
        assertEquals(TXID, response.getTransactionID());

        /* state should be reset to have no known ongoing transaction or working peer set */
        assertEquals(NO_ONGOING_TRANSACTION, participantSM.getOngoingTransactionID());
        assertNull(participantSM.getPeerSet());
        assertNull(participantSM.getUpSet());
    }

    private void assertStateAfterYESSentToCoordinator() {
        assertThat(participantUnderTest.getDtLog().getLogAsString(), containsString("YES"));
        /* YES was sent to coordinator */
        assertEquals(1, peerToCoordinator.getOutQueue().size());
        Message response = coordinatorToPeer.receiveMessage();
        assertTrue(Message.Command.YES.equals(response.getCommand()));
        assertEquals(TXID, response.getTransactionID());

        /* state should be reset to have no known ongoing transaction or working peer set */
        assertEquals(TXID, participantSM.getOngoingTransactionID());
        assertEquals(A_PEER_REFS, participantSM.getPeerSet());
        assertEquals(A_PEER_REFS, participantSM.getUpSet());
    }

    @Test
    public void testReceiveValidAddRequest() throws Exception {

        final AddRequest msg = new AddRequest(A_SONG_TUPLE, TXID, A_PEER_REFS);
        testReceiveFromCoordinator(msg);

        /* log after an ADD

        ADD txnID
        songName
        url
        PEERS numPeers id1 port1 id2 port2
        YES

        * */

        /* assert log format is correct */
        final String[] logLines = participantUnderTest.getDtLog().getLogAsString().split("\n");
        assertThat(logLines[0], containsString("ADD "+TXID));
        assertThat(logLines[1], containsString(A_SONG_NAME));
        assertThat(logLines[2], containsString(A_URL));
        assertThat(logLines[3], containsString("PEERS 3 2 2 3 3 4 4"));
        assertThat(logLines[4], containsString("YES"));

        /* assert that it was sent to the coordinator */
        assertEquals(1, peerToCoordinator.getOutQueue().size());
        assertTrue(YES.equals(coordinatorToPeer.receiveMessage().getCommand()));

        /* this transaction has been set as ONGOING */
        assertEquals(msg.getTransactionID(), participantSM.getOngoingTransactionID());
        assertEquals(msg.getPeerSet(), participantSM.getPeerSet());
    }


    @Test
    public void testReceiveInvalidAddRequest_existingSong() throws Exception {

        /* node already has the song */
        participantUnderTest.addSong(A_SONG_TUPLE);

        /* then receives a request to add it */
        final AddRequest msg = new AddRequest(A_SONG_TUPLE, TXID, A_PEER_REFS);
        testReceiveFromCoordinator(msg);

        /* assert log format is correct */
        final String[] logLines = participantUnderTest.getDtLog().getLogAsString().split("\n");
        assertThat(logLines[0], containsString("ADD "+TXID));
        assertThat(logLines[1], containsString(A_SONG_NAME));
        assertThat(logLines[2], containsString(A_URL));
        assertThat(logLines[3], containsString("PEERS 3 2 2 3 3 4 4"));
        assertThat(logLines[4], containsString("ABORT"));

        assertStateAfterNOSentToCoordinator();
    }

    @Test
    public void testReceiveInvalidAddRequest_sameName_differentURL() throws Exception {

        /* node already has the song */
        participantUnderTest.addSong(A_SONG_TUPLE);

        /* then receives a request to add the same song with a new URL */
        final AddRequest msg = new AddRequest(SAME_SONG_NEW_URL, TXID, A_PEER_REFS);
        testReceiveFromCoordinator(msg);

        assertStateAfterNOSentToCoordinator();
    }

    @Test
    public void testReceiveInvalidUpdateRequest_nonexistentSong() throws Exception {
        /* this will be invalid because the participantSM under test doesn't have a song by this name */
        Message msg = new UpdateRequest(A_SONG_NAME, new SongTuple("newName", A_URL), TXID, A_PEER_REFS);

        /* we have to receive the msg so that we can
         * tell the coordinator which msg we're referring to */
        testReceiveFromCoordinator(msg);

        assertStateAfterNOSentToCoordinator();
    }

    @Test
    public void testReceiveValidUpdateRequest() throws Exception {
        /* node already has the song */
        participantUnderTest.addSong(A_SONG_TUPLE);

        /* same name new URL */
        Message msg = new UpdateRequest(A_SONG_NAME, SAME_SONG_NEW_URL, TXID, A_PEER_REFS);
        testReceiveFromCoordinator(msg);

        /* correct log format after UPDATE

            UPDATE txnID
            old song name
            new song name
            new url
            PEERS numPeers id1 port1 id2 port2
            YES or ABORT

         */

        /* assert log state */
        final String[] logLines = participantUnderTest.getDtLog().getLogAsString().split("\n");
        assertThat(logLines[0], containsString("UPDATE "+TXID));
        assertThat(logLines[1], containsString(A_SONG_NAME));
        assertThat(logLines[2], containsString(A_SONG_NAME));
        assertThat(logLines[3], containsString(DIFFERENT_URL));
        assertThat(logLines[4], containsString("PEERS 3 2 2 3 3 4 4"));
        assertThat(logLines[5], containsString("YES"));

        /* assert channel state and node states are correct */
        assertStateAfterYESSentToCoordinator();
    }

    @Test
    public void testReceiveValidDeleteRequest() throws Exception {
        /* node already has the song */
        participantUnderTest.addSong(A_SONG_TUPLE);

        Message msg = new DeleteRequest(A_SONG_NAME, TXID, A_PEER_REFS);
        testReceiveFromCoordinator(msg);

        /* correct log format after DELETE

            DELETE txnID
            song name
            PEERS numPeers id1 port1 id2 port2
            YES or ABORT

         */

        /* assert log state */
        final String[] logLines = participantUnderTest.getDtLog().getLogAsString().split("\n");
        assertThat(logLines[0], containsString("DELETE "+TXID));
        assertThat(logLines[1], containsString(A_SONG_NAME));
        assertThat(logLines[2], containsString("PEERS 3 2 2 3 3 4 4"));
        assertThat(logLines[3], containsString("YES"));

        /* assert channel state and node states are correct */
        assertStateAfterYESSentToCoordinator();
    }

    @Test
    public void testReceiveValidPrecommit() throws Exception {

        /* set up and verify the before-state */
        participantSM.setOngoingTransactionID(TXID);
        assertFalse(participantSM.isPrecommitted());
        assertEquals(TXID, participantSM.getOngoingTransactionID());

        final Message msg = new PrecommitRequest(TXID);
        testReceiveFromCoordinator(msg);

        assertTrue(participantSM.isPrecommitted());
        assertEquals(TXID, participantSM.getOngoingTransactionID());

        /* channel state should have an ACK in it */
        assertEquals(1, coordinatorToPeer.getInQueue().size());
        assertEquals(ACK, coordinatorToPeer.receiveMessage().getCommand());

        /* log should be "empty" (from the perspective of this one action) */
        assertEquals("", participantUnderTest.getDtLog().getLogAsString());
    }

    @Test
    public void testReceiveValidCommit_addRequest() throws Exception {

        /* proper precommit state for add request */
        participantSM.setOngoingTransactionID(TXID);
        participantSM.setPrecommitted(true);
        final VoteRequest action = new AddRequest(A_SONG_TUPLE, TXID, A_PEER_REFS);
        participantSM.setAction(action);

        /* receive commit request */
        final Message msg = new CommitRequest(TXID);
        testReceiveFromCoordinator(msg);

        /* assert correct resulting state */
        assertThat(participantUnderTest.getDtLog().getLogAsString(), containsString("COMMIT"));
        assertEquals(NO_ONGOING_TRANSACTION, participantSM.getOngoingTransactionID());
        assertTrue(peerToCoordinator.getOutQueue().isEmpty());

        /* including action performed */
        assertTrue(participantUnderTest.hasExactSongTuple(A_SONG_TUPLE));
    }

    @Test
    public void testReceiveValidCommit_updateRequest() throws Exception {

        /* proper precommit state for update request */
        participantUnderTest.addSong(A_SONG_TUPLE);
        assertTrue(participantUnderTest.hasExactSongTuple(A_SONG_TUPLE));
        participantSM.setOngoingTransactionID(TXID);
        participantSM.setPrecommitted(true);
        final VoteRequest action = new UpdateRequest(A_SONG_NAME, SAME_SONG_NEW_URL, TXID, A_PEER_REFS);
        participantSM.setAction(action);

        /* receive commit request */
        final Message msg = new CommitRequest(TXID);
        testReceiveFromCoordinator(msg);

        /* assert correct resulting state */
        assertThat(participantUnderTest.getDtLog().getLogAsString(), containsString("COMMIT"));
        assertEquals(NO_ONGOING_TRANSACTION, participantSM.getOngoingTransactionID());
        assertTrue(peerToCoordinator.getOutQueue().isEmpty());

        /* including action performed */
        assertTrue(participantUnderTest.hasExactSongTuple(SAME_SONG_NEW_URL));
        assertFalse(participantUnderTest.hasExactSongTuple(A_SONG_TUPLE));
    }

    @Test
    public void testReceiveValidCommit_deleteRequest() throws Exception {

        /* proper precommit state for delete request */
        participantUnderTest.addSong(A_SONG_TUPLE);
        assertTrue(participantUnderTest.hasExactSongTuple(A_SONG_TUPLE));
        participantSM.setOngoingTransactionID(TXID);
        participantSM.setPrecommitted(true);
        final VoteRequest action = new DeleteRequest(A_SONG_NAME, TXID, A_PEER_REFS);
        participantSM.setAction(action);

        /* receive commit request */
        final Message msg = new CommitRequest(TXID);
        testReceiveFromCoordinator(msg);

        /* assert correct resulting state */
        assertThat(participantUnderTest.getDtLog().getLogAsString(), containsString("COMMIT"));
        assertEquals(NO_ONGOING_TRANSACTION, participantSM.getOngoingTransactionID());
        assertTrue(peerToCoordinator.getOutQueue().isEmpty());

        /* including action performed */
        assertFalse(participantUnderTest.hasExactSongTuple(SAME_SONG_NEW_URL));
        assertFalse(participantUnderTest.hasExactSongTuple(A_SONG_TUPLE));
    }

    @Test
    public void testReceiveAbortAfterVotingYES() throws Exception {

        /* set up pre-abort msg state */
        participantSM.setOngoingTransactionID(TXID);
        final VoteRequest action = new AddRequest(A_SONG_TUPLE, TXID, A_PEER_REFS);
        participantSM.setAction(action);
        participantSM.setPeerSet(A_PEER_REFS);

        /* receive abort msg */
        final Message msg = new Message(ABORT, TXID);
        testReceiveFromCoordinator(msg);

        /* assert correct resulting state */
        assertEquals(NO_ONGOING_TRANSACTION, participantSM.getOngoingTransactionID());
        assertNull(participantSM.getAction());
        assertNull(participantSM.getPeerSet());
        assertNull(participantSM.getUpSet());
        assertThat(participantUnderTest.getDtLog().getLogAsString(), containsString("ABORT"));
    }

    /**
     * This likely is incorrect because not enough of the async setup has been fleshed-out
     * to properly know how to emulate the triggering of the heartbeat and what really
     * should happen. This is a guess though.
     */
    @Test
    public void testCoordinatorTimeoutOnHeartbeat() throws Exception {

        PriorityQueue<PeerReference> queue = new PriorityQueue<>(A_PEER_REFS);
        int coordinatorID = queue.poll().getNodeID();
        int nextCoordID = queue.poll().getNodeID();

        /* timeout has triggered */
        participantSM.setPeerSet(A_PEER_REFS);
        int peerRefsInitialSize = A_PEER_REFS.size();

        participantSM.setUpSet(participantSM.getPeerSet()
                        .stream()
                        .map(PeerReference::clone)
                        .collect(Collectors.toList()));

        participantSM.setOngoingTransactionID(TXID);

        participantSM.coordinatorTimeoutOnHeartbeat(coordinatorID);


        /* so the following state changes have occurred */

        /* failed coordinator was removed from upSet but not peerSet */
        assertEquals(peerRefsInitialSize-1, participantSM.getUpSet().size());
        assertEquals(peerRefsInitialSize, participantSM.getPeerSet().size());

        /* this fact was logged */
        final String logAsString = participantUnderTest.getDtLog().getLogAsString();
        assertThat(logAsString, containsString("TIMEOUT "+coordinatorID));

        /* connection established to the lowest node still in upSet */

        QueueConnection qConn = (QueueConnection)
                participantUnderTest.getPeerConns()
                        .stream()
                        .filter(conn -> conn.getReceiverID() == nextCoordID)
                        .findFirst()
                        .get();

        /* sent it a UR_ELECTED message */
        assertEquals(1, qConn.getOutQueue().size());
        assertEquals(UR_ELECTED, qConn.getOutQueue().peek().getCommand());
        assertEquals(TXID, qConn.getOutQueue().peek().getTransactionID());
    }

    @Test
    public void testReceiveUR_ELECTED() throws Exception {

        testReceiveFromCoordinator(new Message(UR_ELECTED, TXID));

        assertTrue(participantUnderTest.getStateMachine() instanceof CoordinatorStateMachine);

        /* TODO after the CoordinatorStateMachine has been fleshed-we'll need to verify
         *      that the transaction ID and command-action etc. have remained the same */
    }

    @Test
    public void testReceiveDUB_COORDINATOR() throws Exception {
        testReceiveFromCoordinator(new Message(DUB_COORDINATOR, -1));
        assertTrue(participantUnderTest.getStateMachine() instanceof CoordinatorStateMachine);
    }
}
