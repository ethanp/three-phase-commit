package node;

import messages.CommitRequest;
import messages.Message;
import messages.PrecommitRequest;
import messages.vote_req.AddRequest;
import messages.vote_req.UpdateRequest;
import node.mock.MockParticipant;
import org.junit.Before;
import org.junit.Test;
import util.SongTuple;

import java.util.Arrays;
import java.util.Collection;

import static node.ParticipantStateMachine.NO_ONGOING_TRANSACTION;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ParticipantStateMachineTest {

    MockParticipant mockParticipant;
    ParticipantStateMachine psm;
    final static int TXID = 1;
    final static Collection<PeerReference> DEFAULT_PEER_REFS = Arrays.asList(
            new PeerReference(2, 2),
            new PeerReference(3, 3),
            new PeerReference(4, 4));

    @Before
    public void setUp() throws Exception {
        mockParticipant = new MockParticipant();
        psm = mockParticipant.getStateMachine();
    }

    @Test
    public void testReceiveUpdateRequestForNonexistentSong() throws Exception {
        /* this will be invalid because the psm under test doesn't have a song by this name */
        Message message = new UpdateRequest("name", new SongTuple("name", "url"), TXID, DEFAULT_PEER_REFS);

        /* we have to receive the msg so that we can
         * tell the coordinator which msg we're referring to */
        psm.receiveMessage(message);

        assertResponseToInvalidRequest();
    }

    @Test
    public void testReceiveValidAddRequest() throws Exception {

        final AddRequest msg = new AddRequest(new SongTuple("name", "url"), TXID, DEFAULT_PEER_REFS);
        psm.receiveMessage(msg);

        assertThat(mockParticipant.getDtLog().getLogAsString(), containsString("YES"));
        assertEquals(1, mockParticipant.getSentMessages().size());
        assertTrue(Message.Command.YES.equals(mockParticipant.getSentMessages().get(0).getCommand()));

        /* this transaction has been set as ONGOING */
        assertEquals(msg.getTransactionID(), psm.getOngoingTransactionID());
        assertEquals(msg.getPeerSet(), psm.getWorkingPeerSet());
    }

    @Test
    public void testReceiveExistingAddRequest() throws Exception {
        final SongTuple song = new SongTuple("existing", "url");

        /* node already has the song */
        mockParticipant.addSong(song);

        /* then receives a request to add it */
        psm.receiveMessage(new AddRequest(song, TXID, DEFAULT_PEER_REFS));
        assertResponseToInvalidRequest();
    }

    @Test
    public void testReceiveAddSameNameDifferentURL() throws Exception {
        final String name = "existing";
        final SongTuple existingOldURL = new SongTuple(name, "url1");
        final SongTuple existingNewURL = new SongTuple(name, "url2");

        /* node already has the song */
        mockParticipant.addSong(existingOldURL);

        /* then receives a request to add the same song with a new URL */
        psm.receiveMessage(new AddRequest(existingNewURL, TXID, DEFAULT_PEER_REFS));

        assertResponseToInvalidRequest();
    }

    private void assertResponseToInvalidRequest() {

        /* ABORT has been logged */
        assertThat(mockParticipant.getDtLog().getLogAsString(), containsString("ABORT"));

        /* NO was sent to coordinator */
        final Message response = mockParticipant.getSentMessages().get(0);
        assertTrue(Message.Command.NO.equals(response.getCommand()));
        assertEquals(TXID, response.getTransactionID());

        /* node should have no known ongoing transaction */
        assertEquals(NO_ONGOING_TRANSACTION, psm.getOngoingTransactionID());
        assertNull(psm.getWorkingPeerSet());
    }

    @Test
    public void testReceivePrecommitForNonOngoingTransaction() throws Exception {

        psm.receiveMessage(new PrecommitRequest(TXID));

        // TODO what happens in this situation?

    }

    @Test
    public void testReceiveValidPrecommit() throws Exception {

        psm.setOngoingTransactionID(TXID);

        assertFalse(psm.isPrecommitted());
        assertEquals(TXID, psm.getOngoingTransactionID());

        psm.receiveMessage(new PrecommitRequest(TXID));

        assertTrue(psm.isPrecommitted());
        assertEquals(TXID, psm.getOngoingTransactionID());

        /* note: precommit doesn't get logged (Lecture 3, Pg. 13) */
    }

    /**
     * This is still missing the fact that we must to save WHAT the transaction is supposed to DO
     * in the state machine so that it can be applied upon receiving commit.
     */

    @Test
    public void testReceiveValidCommit() throws Exception {
        psm.setOngoingTransactionID(TXID);
        psm.setPrecommitted(true);

        psm.receiveMessage(new CommitRequest(TXID));

        /* logged COMMIT */
        assertThat(mockParticipant.getDtLog().getLogAsString(), containsString("COMMIT"));

        assertEquals(NO_ONGOING_TRANSACTION, psm.getOngoingTransactionID());
    }
}
