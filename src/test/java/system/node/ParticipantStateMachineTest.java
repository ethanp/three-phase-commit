package system.node;

import org.junit.Test;

public class ParticipantStateMachineTest {
    @Test
    public void testReceiveInvalidVoteRequest() throws Exception {
        ParticipantStateMachine psm = new ParticipantStateMachine();
        psm.receiveInvalidVoteRequest();

        /* assert ABORT has been logged */

        /* assert msg NO was sent to coordinator */

        /* assert that we're back in our original state */
    }

}
