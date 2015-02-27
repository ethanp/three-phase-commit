package node;

import messages.AddRequest;
import messages.Message;
import messages.UpdateRequest;
import node.mock.MockParticipant;
import org.junit.Before;
import org.junit.Test;
import util.SongTuple;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
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
    public void testReceiveInvalidVoteRequest() throws Exception {

        /* an UpdateRequest is-a VoteRequest */

        /* this will be invalid because the psm under test doesn't have a song by this name */
        Message message = new UpdateRequest("name", new SongTuple("name", "url"), 1);

        /* we have to receive the msg so that we can
         * tell the coordinator which msg we're referring to */
        psm.receiveVoteRequest(message);

        /* assert ABORT has been logged */
        assertThat(mockParticipant.getDtLog().getLogAsString(), containsString("ABORT"));

        /* assert msg NO was sent to coordinator */
        final Message response = mockParticipant.getSentMessages().get(0);
        assertTrue(Message.Command.NO.equals(response.getCommand()));
        assertEquals(1, response.getMsgId());

        /* TODO assert that we're back in our original state */
        /* how does one do that? */
    }

    @Test
    public void testReceiveValidVoteRequest() throws Exception {
        Message message = new AddRequest(new SongTuple("name", "url"), 1);

        psm.receiveVoteRequest(message);

        assertThat(mockParticipant.getDtLog().getLogAsString(), containsString("YES"));
        assertEquals(1, mockParticipant.getSentMessages().size());
        assertTrue(Message.Command.YES.equals(mockParticipant.getSentMessages().get(0).getCommand()));
    }
}
