package node;

import messages.AbortRequest;
import messages.CommitRequest;
import messages.DubCoordinatorMessage;
import messages.ElectedMessage;
import messages.Message;
import messages.PeerTimeout;
import messages.PrecommitRequest;
import messages.YesResponse;
import messages.vote_req.AddRequest;
import messages.vote_req.DeleteRequest;
import messages.vote_req.UpdateRequest;
import messages.vote_req.VoteRequest;
import node.system.SyncNode;

import org.junit.Before;
import org.junit.Test;

import system.network.Connection;
import system.network.QueueConnection;
import system.network.QueueSocket;
import util.SongTuple;
import util.TestCommon;

import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import javax.jws.Oneway;

import static messages.Message.Command.ABORT;
import static messages.Message.Command.ACK;
import static messages.Message.Command.DUB_COORDINATOR;
import static messages.Message.Command.UR_ELECTED;
import static messages.Message.Command.YES;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static util.Common.NO_ONGOING_TRANSACTION;

public class ParticipantStateMachineTest extends TestCommon {

    SyncNode syncNode;
    ParticipantStateMachine psm;
    QueueSocket queueSocket;
    QueueConnection peerToCoordinator;
    QueueConnection coordinatorToPeer;

    @Before
    public void setUp() throws Exception {
        syncNode = new SyncNode(TEST_PEER_ID, null);
        psm = (ParticipantStateMachine) syncNode.getStateMachine();
        queueSocket = new QueueSocket(TEST_COORD_ID, TEST_PEER_ID);
        peerToCoordinator = queueSocket.getConnectionToAID();
        syncNode.addConnection(peerToCoordinator);
        coordinatorToPeer = queueSocket.getConnectionToBID();
    }

    private void testReceiveFromCoordinator(Message message) {
        coordinatorToPeer.sendMessage(message);
        assertTrue(psm.receiveMessage(peerToCoordinator));
    }

    private void assertStateAfterNOSentToCoordinator() {
        assertThat(syncNode.getDtLog().getLogAsString(), containsString("ABORT"));
        /* NO was sent to coordinator */
        assertEquals(1, peerToCoordinator.getOutQueue().size());
        Message response = coordinatorToPeer.receiveMessage();
        assertTrue(Message.Command.NO.equals(response.getCommand()));
        assertEquals(TXID, response.getTransactionID());

        /* state should be reset to have no known ongoing transaction or working peer set */
        assertEquals(NO_ONGOING_TRANSACTION, psm.getOngoingTransactionID());
        assertNull(psm.getPeerSet());
        assertNull(psm.getUpSet());
    }

    private void assertStateAfterYESSentToCoordinator() {
        assertThat(syncNode.getDtLog().getLogAsString(), containsString("YES"));
        /* YES was sent to coordinator */
        assertEquals(1, peerToCoordinator.getOutQueue().size());
        Message response = coordinatorToPeer.receiveMessage();
        assertTrue(Message.Command.YES.equals(response.getCommand()));
        assertEquals(TXID, response.getTransactionID());

        /* state should be reset to have no known ongoing transaction or working peer set */
        assertEquals(TXID, psm.getOngoingTransactionID());
        assertEquals(A_PEER_REFS, psm.getPeerSet());
        assertEquals(A_PEER_REFS, psm.getUpSet());
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
        Object[] messages = syncNode.getDtLog().getLoggedMessages().toArray();
        AddRequest add = (AddRequest)messages[0];
        assertTrue(add != null);
        assertEquals(A_SONG_TUPLE, add.getSongTuple());
        YesResponse yes = (YesResponse)messages[1];
        assertTrue(yes != null);

        /* assert that it was sent to the coordinator */
        assertEquals(1, peerToCoordinator.getOutQueue().size());
        assertTrue(YES.equals(coordinatorToPeer.receiveMessage().getCommand()));

        /* this transaction has been set as ONGOING */
        assertEquals(msg.getTransactionID(), psm.getOngoingTransactionID());
        assertEquals(msg.getPeerSet(), psm.getPeerSet());
    }


    @Test
    public void testReceiveInvalidAddRequest_existingSong() throws Exception {

        /* node already has the song */
        syncNode.addSong(A_SONG_TUPLE);

        /* then receives a request to add it */
        final AddRequest msg = new AddRequest(A_SONG_TUPLE, TXID, A_PEER_REFS);
        testReceiveFromCoordinator(msg);

        /* assert log format is correct */
        Object[] messages = syncNode.getDtLog().getLoggedMessages().toArray();
        AddRequest add = (AddRequest)messages[0];
        assertTrue(add != null);
        assertEquals(A_SONG_TUPLE, add.getSongTuple());
        AbortRequest abort = (AbortRequest)messages[1];
        assertTrue(abort != null);

        assertStateAfterNOSentToCoordinator();
    }

    @Test
    public void testReceiveInvalidAddRequest_sameName_differentURL() throws Exception {

        /* node already has the song */
        syncNode.addSong(A_SONG_TUPLE);

        /* then receives a request to add the same song with a new URL */
        final AddRequest msg = new AddRequest(SAME_SONG_NEW_URL, TXID, A_PEER_REFS);
        testReceiveFromCoordinator(msg);

        assertStateAfterNOSentToCoordinator();
    }

    @Test
    public void testReceiveInvalidUpdateRequest_nonexistentSong() throws Exception {
        /* this will be invalid because the psm under test doesn't have a song by this name */
        Message msg = new UpdateRequest(A_SONG_NAME, new SongTuple("newName", A_URL), TXID, A_PEER_REFS);

        /* we have to receive the msg so that we can
         * tell the coordinator which msg we're referring to */
        testReceiveFromCoordinator(msg);

        assertStateAfterNOSentToCoordinator();
    }

    @Test
    public void testReceiveValidUpdateRequest() throws Exception {
        /* node already has the song */
        syncNode.addSong(A_SONG_TUPLE);

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
        Object[] messages = syncNode.getDtLog().getLoggedMessages().toArray();
        UpdateRequest update = (UpdateRequest)messages[0];
        assertTrue(update != null);
        assertEquals(A_SONG_NAME, update.getSongName());
        YesResponse yes = (YesResponse)messages[1];
        assertTrue(yes != null);

        /* assert channel state and node states are correct */
        assertStateAfterYESSentToCoordinator();
    }

    @Test
    public void testReceiveValidDeleteRequest() throws Exception {
        /* node already has the song */
        syncNode.addSong(A_SONG_TUPLE);

        Message msg = new DeleteRequest(A_SONG_NAME, TXID, A_PEER_REFS);
        testReceiveFromCoordinator(msg);

        /* correct log format after DELETE

            DELETE txnID
            song name
            PEERS numPeers id1 port1 id2 port2
            YES or ABORT

         */

        /* assert log state */
        Object[] messages = syncNode.getDtLog().getLoggedMessages().toArray();
        DeleteRequest delete = (DeleteRequest)messages[0];
        assertTrue(delete != null);
        assertEquals(A_SONG_NAME, delete.getSongName());
        YesResponse yes = (YesResponse)messages[1];
        assertTrue(yes != null);

        /* assert channel state and node states are correct */
        assertStateAfterYESSentToCoordinator();
    }

    @Test
    public void testReceiveValidPrecommit() throws Exception {

        /* set up and verify the before-state */
        psm.setOngoingTransactionID(TXID);
        assertFalse(psm.isPrecommitted());
        assertEquals(TXID, psm.getOngoingTransactionID());

        final Message msg = new PrecommitRequest(TXID);
        testReceiveFromCoordinator(msg);

        assertTrue(psm.isPrecommitted());
        assertEquals(TXID, psm.getOngoingTransactionID());

        /* channel state should have an ACK in it */
        assertEquals(1, coordinatorToPeer.getInQueue().size());
        assertEquals(ACK, coordinatorToPeer.receiveMessage().getCommand());

        /* log should be "empty" (from the perspective of this one action) */
        assertEquals("", syncNode.getDtLog().getLogAsString());
    }

    @Test
    public void testReceiveValidCommit_addRequest() throws Exception {

        /* proper precommit state for add request */
        psm.setOngoingTransactionID(TXID);
        psm.setPrecommitted(true);
        final VoteRequest action = new AddRequest(A_SONG_TUPLE, TXID, A_PEER_REFS);
        psm.setAction(action);

        /* receive commit request */
        final Message msg = new CommitRequest(TXID);
        testReceiveFromCoordinator(msg);

        /* assert correct resulting state */
        assertThat(syncNode.getDtLog().getLogAsString(), containsString("COMMIT"));
        assertEquals(NO_ONGOING_TRANSACTION, psm.getOngoingTransactionID());
        assertTrue(peerToCoordinator.getOutQueue().isEmpty());

        /* including action performed */
        assertTrue(syncNode.hasExactSongTuple(A_SONG_TUPLE));
    }

    @Test
    public void testReceiveValidCommit_updateRequest() throws Exception {

        /* proper precommit state for update request */
        syncNode.addSong(A_SONG_TUPLE);
        assertTrue(syncNode.hasExactSongTuple(A_SONG_TUPLE));
        psm.setOngoingTransactionID(TXID);
        psm.setPrecommitted(true);
        final VoteRequest action = new UpdateRequest(A_SONG_NAME, SAME_SONG_NEW_URL, TXID, A_PEER_REFS);
        psm.setAction(action);

        /* receive commit request */
        final Message msg = new CommitRequest(TXID);
        testReceiveFromCoordinator(msg);

        /* assert correct resulting state */
        assertThat(syncNode.getDtLog().getLogAsString(), containsString("COMMIT"));
        assertEquals(NO_ONGOING_TRANSACTION, psm.getOngoingTransactionID());
        assertTrue(peerToCoordinator.getOutQueue().isEmpty());

        /* including action performed */
        assertTrue(syncNode.hasExactSongTuple(SAME_SONG_NEW_URL));
        assertFalse(syncNode.hasExactSongTuple(A_SONG_TUPLE));
    }

    @Test
    public void testReceiveValidCommit_deleteRequest() throws Exception {

        /* proper precommit state for delete request */
        syncNode.addSong(A_SONG_TUPLE);
        assertTrue(syncNode.hasExactSongTuple(A_SONG_TUPLE));
        psm.setOngoingTransactionID(TXID);
        psm.setPrecommitted(true);
        final VoteRequest action = new DeleteRequest(A_SONG_NAME, TXID, A_PEER_REFS);
        psm.setAction(action);

        /* receive commit request */
        final Message msg = new CommitRequest(TXID);
        testReceiveFromCoordinator(msg);

        /* assert correct resulting state */
        assertThat(syncNode.getDtLog().getLogAsString(), containsString("COMMIT"));
        assertEquals(NO_ONGOING_TRANSACTION, psm.getOngoingTransactionID());
        assertTrue(peerToCoordinator.getOutQueue().isEmpty());

        /* including action performed */
        assertFalse(syncNode.hasExactSongTuple(SAME_SONG_NEW_URL));
        assertFalse(syncNode.hasExactSongTuple(A_SONG_TUPLE));
    }

    @Test
    public void testReceiveAbortAfterVotingYES() throws Exception {

        /* set up pre-abort msg state */
        psm.setOngoingTransactionID(TXID);
        final VoteRequest action = new AddRequest(A_SONG_TUPLE, TXID, A_PEER_REFS);
        psm.setAction(action);
        psm.setPeerSet(A_PEER_REFS);

        /* receive abort msg */
        final Message msg = new AbortRequest(TXID);
        testReceiveFromCoordinator(msg);

        /* assert correct resulting state */
        assertEquals(NO_ONGOING_TRANSACTION, psm.getOngoingTransactionID());
        assertNull(psm.getAction());
        assertNull(psm.getPeerSet());
        assertNull(psm.getUpSet());
        assertThat(syncNode.getDtLog().getLogAsString(), containsString("ABORT"));
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
        psm.setCoordinatorID(coordinatorID);
        psm.setPeerSet(A_PEER_REFS);
        int peerRefsInitialSize = A_PEER_REFS.size();
        psm.setUpSet(psm.getPeerSet().stream().map(PeerReference::clone).collect(Collectors.toList()));
        psm.setOngoingTransactionID(TXID);

        testReceiveFromCoordinator(new PeerTimeout(coordinatorID));
        
        /* so the following state changes have occurred */

        /* failed coordinator was removed from upSet but not peerSet */
        assertEquals(peerRefsInitialSize-1, psm.getUpSet().size());
        assertEquals(peerRefsInitialSize, psm.getPeerSet().size());

        /* this fact was logged */
        Message lastLoggedMessage = (Message)syncNode.getDtLog().getLoggedMessages().toArray()[0];
        assertTrue(lastLoggedMessage instanceof PeerTimeout);

        /* connection established to the lowest node still in upSet */
        Iterator<Connection> connectionIterator = syncNode.getPeerConns().iterator();
        Connection newCoordConn = null;
        while (connectionIterator.hasNext()) {
            Connection conn = connectionIterator.next();
            if (conn.getReceiverID() == nextCoordID) {
                newCoordConn = conn;
                break;
            }
        }
        assertNotNull(newCoordConn);

        QueueConnection qConn = (QueueConnection) newCoordConn;

        /* sent it a UR_ELECTED message */
        assertEquals(1, qConn.getOutQueue().size());
        assertEquals(UR_ELECTED, qConn.getOutQueue().peek().getCommand());
        assertEquals(TXID, qConn.getOutQueue().peek().getTransactionID());
    }

    @Test
    public void testReceiveUR_ELECTED() throws Exception {

        testReceiveFromCoordinator(new ElectedMessage(TXID));

        assertTrue(syncNode.getStateMachine() instanceof CoordinatorStateMachine);

        /* TODO after the CoordinatorStateMachine has been fleshed-we'll need to verify
         *      that the transaction ID and command-action etc. have remained the same */
    }

    @Test
    public void testReceiveDUB_COORDINATOR() throws Exception {
        testReceiveFromCoordinator(new DubCoordinatorMessage());
        assertTrue(syncNode.getStateMachine() instanceof CoordinatorStateMachine);
    }
}
