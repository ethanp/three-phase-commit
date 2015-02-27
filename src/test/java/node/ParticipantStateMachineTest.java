package node;

import messages.Message;
import messages.UpdateRequest;
import node.mock.MockParticipant;
import org.junit.Test;
import util.SongTuple;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ParticipantStateMachineTest {
    @Test
    public void testReceiveInvalidVoteRequest() throws Exception {

        /* create a LocalNode extends Node that "logs" to a ByteArrayInputStream */

        MockParticipant mockParticipant = new MockParticipant();
        ParticipantStateMachine psm = mockParticipant.getStateMachine();

        /* this will be invalid because the psm under test doesn't have a song by this name */
        Message message = new UpdateRequest("name", new SongTuple("name", "url"), 1);

        /* we have to receive the msg so that we can
         * tell the coordinator which msg we're referring to */
        psm.receiveInvalidVoteRequest(message);

        /* assert ABORT has been logged */
        assertThat(mockParticipant.getDtLog().getLogAsString(), containsString("ABORT"));

        /* assert msg NO was sent to coordinator */
        final Message response = mockParticipant.getSentMessages().get(0);
        assertTrue(response.getCommand().equals(Message.Command.NO));
        assertEquals(1, response.getMsgId());

        /* TODO assert that we're back in our original state */
    }

    @Test
    public void testReceiveValidVoteRequest() throws Exception {

    }
}
