package node;

import messages.AddRequest;
import messages.Message;
import messages.PrecommitRequest;
import messages.UpdateRequest;
import node.mock.MockParticipant;
import org.junit.Before;
import org.junit.Test;
import util.SongTuple;

import static node.ParticipantStateMachine.NO_ONGOING_TRANSACTION;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ParticipantStateMachineTest {

    MockParticipant mockParticipant;
    ParticipantStateMachine psm;

    @Before
    public void setUp() throws Exception {
        mockParticipant = new MockParticipant();
        psm = mockParticipant.getStateMachine();
    }

    @Test
    public void testReceiveUpdateRequestForNonexistentSong() throws Exception {
        /* this will be invalid because the psm under test doesn't have a song by this name */
        Message message = new UpdateRequest("name", new SongTuple("name", "url"), 1);

        /* we have to receive the msg so that we can
         * tell the coordinator which msg we're referring to */
        psm.receiveMessage(message);

        assertResponseToInvalidRequest();
    }

    @Test
    public void testReceiveValidAddRequest() throws Exception {

        final AddRequest msg = new AddRequest(new SongTuple("name", "url"), 1);
        psm.receiveMessage(msg);

        assertThat(mockParticipant.getDtLog().getLogAsString(), containsString("YES"));
        assertEquals(1, mockParticipant.getSentMessages().size());
        assertTrue(Message.Command.YES.equals(mockParticipant.getSentMessages().get(0).getCommand()));

        /* this transaction has been set as ONGOING */
        assertEquals(msg.getTransactionID(), psm.getOngoingTransactionID());
    }

    @Test
    public void testReceiveExistingAddRequest() throws Exception {
        final SongTuple song = new SongTuple("existing", "url");

        /* node already has the song */
        mockParticipant.addSong(song);

        /* then receives a request to add it */
        psm.receiveMessage(new AddRequest(song, 1));
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
        psm.receiveMessage(new AddRequest(existingNewURL, 1));

        assertResponseToInvalidRequest();
    }

    private void assertResponseToInvalidRequest() {

        /* ABORT has been logged */
        assertThat(mockParticipant.getDtLog().getLogAsString(), containsString("ABORT"));

        /* NO was sent to coordinator */
        final Message response = mockParticipant.getSentMessages().get(0);
        assertTrue(Message.Command.NO.equals(response.getCommand()));
        assertEquals(1, response.getTransactionID());

        /* node should have no known ongoing transaction */
        assertEquals(NO_ONGOING_TRANSACTION, psm.getOngoingTransactionID());
    }

    @Test
    public void testReceivePrecommitForNonOngoingTransaction() throws Exception {

        psm.receiveMessage(new PrecommitRequest(1));

        // TODO what happens in this situation?

    }

    @Test
    public void testReceiveValidPrecommit() throws Exception {

        final int TXID = 1;
        psm.setOngoingTransactionID(TXID);

        assertFalse(psm.isPrecommitted());
        assertEquals(TXID, psm.getOngoingTransactionID());

        psm.receiveMessage(new PrecommitRequest(TXID));

        assertTrue(psm.isPrecommitted());
        assertEquals(TXID, psm.getOngoingTransactionID());
    }
}
