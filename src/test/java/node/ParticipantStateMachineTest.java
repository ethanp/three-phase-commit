package node;

import messages.CommitRequest;
import messages.Message;
import messages.PrecommitRequest;
import messages.vote_req.AddRequest;
import messages.vote_req.UpdateRequest;
import node.system.SyncNode;
import org.junit.Before;
import org.junit.Test;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import system.network.QueueConnection;
import system.network.QueueSocket;
import util.SongTuple;
import util.TestCommon;

import static messages.Message.Command.YES;
import static util.Common.NO_ONGOING_TRANSACTION;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

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

    @Test
    public void testReceiveValidAddRequest() throws Exception {

        final AddRequest msg = new AddRequest(A_SONG_TUPLE, TXID, A_PEER_REFS);
        assertTrue(testReceiveFromCoordinator(msg));

        /* log after an ADD

        ADD txnID
        songName
        url
        PEERS numPeers id1 port1 id2 port2
        YES

        * */

        /* log format */
        final String[] logLines = syncNode.getDtLog().getLogAsString().split("\n");
        assertThat(logLines[0], containsString("ADD "+TXID));
        assertThat(logLines[1], containsString(A_SONG_NAME));
        assertThat(logLines[2], containsString(A_URL));
        assertThat(logLines[3], containsString("PEERS 3 2 2 3 3 4 4"));
        assertThat(logLines[4], containsString("YES"));

        /* assert that it was sent to the coordinator */
        assertEquals(1, peerToCoordinator.getOutQueue().size());
        assertTrue(YES.equals(coordinatorToPeer.receiveMessage().getCommand()));

        /* this transaction has been set as ONGOING */
        assertEquals(msg.getTransactionID(), psm.getOngoingTransactionID());
        assertEquals(msg.getPeerSet(), psm.getWorkingPeerSet());
    }

    private boolean testReceiveFromCoordinator(Message message) {
        coordinatorToPeer.sendMessage(message);
        return psm.receiveMessage(peerToCoordinator);
    }

    @Test
    public void testReceiveInvalidAddRequest_existingSong() throws Exception {
        final SongTuple song = new SongTuple("existing", "url");

        /* node already has the song */
        syncNode.addSong(song);

        /* then receives a request to add it */
        final AddRequest msg = new AddRequest(song, TXID, A_PEER_REFS);
        assertTrue(testReceiveFromCoordinator(msg));
        assertResponseToInvalidVoteRequest();
    }

    @Test
    public void testReceiveInvalidAddRequest_sameName_differentURL() throws Exception {
        final SongTuple existingOldURL = new SongTuple(A_SONG_NAME, "url1");
        final SongTuple existingNewURL = new SongTuple(A_SONG_NAME, "url2");

        /* node already has the song */
        syncNode.addSong(existingOldURL);

        /* then receives a request to add the same song with a new URL */
        final AddRequest msg = new AddRequest(existingNewURL, TXID, A_PEER_REFS);
        assertTrue(testReceiveFromCoordinator(msg));

        assertResponseToInvalidVoteRequest();
    }

    @Test
    public void testReceiveInvalidUpdateRequest_nonexistentSong() throws Exception {
        /* this will be invalid because the psm under test doesn't have a song by this name */
        Message msg = new UpdateRequest(A_SONG_NAME, new SongTuple("newName", A_URL), TXID, A_PEER_REFS);

        /* we have to receive the msg so that we can
         * tell the coordinator which msg we're referring to */
        assertTrue(testReceiveFromCoordinator(msg));

        assertResponseToInvalidVoteRequest();
    }

    @Test
    public void testReceiveValidUpdateRequest() throws Exception {
        throw new NotImplementedException();
    }

    @Test
    public void testReceiveValidDeleteRequest() throws Exception {
        throw new NotImplementedException();
    }

    private void assertResponseToInvalidVoteRequest() {

        /* ABORT has been logged */
        assertThat(syncNode.getDtLog().getLogAsString(), containsString("ABORT"));

        /* NO was sent to coordinator */
        Message response = null;
        assertTrue(Message.Command.NO.equals(response.getCommand()));
        assertEquals(TXID, response.getTransactionID());

        /* node should have no known ongoing transaction */
        assertEquals(NO_ONGOING_TRANSACTION, psm.getOngoingTransactionID());
        assertNull(psm.getWorkingPeerSet());
    }

    @Test
    public void testReceiveValidPrecommit() throws Exception {

        psm.setOngoingTransactionID(TXID);

        assertFalse(psm.isPrecommitted());
        assertEquals(TXID, psm.getOngoingTransactionID());

        final PrecommitRequest request = new PrecommitRequest(TXID);
        psm.receiveMessage(null);

        assertTrue(psm.isPrecommitted());
        assertEquals(TXID, psm.getOngoingTransactionID());

        /* TODO we're suppose to ACK here */
        throw new NotImplementedException();

        /* note: precommit doesn't get logged (Lecture 3, Pg. 13) */
    }

    @Test
    public void testReceiveValidCommit_addRequest() throws Exception {
        psm.setOngoingTransactionID(TXID);
        psm.setPrecommitted(true);

        final CommitRequest commitRequest = new CommitRequest(TXID);
        psm.receiveMessage(null);

        /* logged COMMIT */
        assertThat(syncNode.getDtLog().getLogAsString(), containsString("COMMIT"));

        assertEquals(NO_ONGOING_TRANSACTION, psm.getOngoingTransactionID());

        /* TODO assert performed action */
        throw new NotImplementedException();
    }

    @Test
    public void testReceiveValidCommit_updateRequest() throws Exception {
        throw new NotImplementedException();
    }

    @Test
    public void testReceiveValidCommit_deleteRequest() throws Exception {
        throw new NotImplementedException();
    }

    @Test
    public void testReceiveAbortAfterVoting() throws Exception {
        throw new NotImplementedException();
    }

    @Test
    public void testCoordinatorTimeoutOnHeartbeat() throws Exception {
        throw new NotImplementedException();
    }

    @Test
    public void testReceiveUR_ELECTED() throws Exception {
        throw new NotImplementedException();
    }

    @Test
    public void testReceiveDUB_COORDINATOR() throws Exception {
        throw new NotImplementedException();
    }
}
