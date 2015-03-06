package console;

import messages.Message;
import messages.vote_req.AddRequest;
import messages.vote_req.DeleteRequest;
import messages.vote_req.UpdateRequest;
import messages.vote_req.VoteRequest;
import org.junit.Test;
import system.failures.DeathAfter;
import system.failures.Failure;
import system.failures.PartialBroadcast;
import util.TestCommon;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CommandConsoleTest extends TestCommon {
    @Test
    public void testAddRequest() throws Exception {
        String cmdStr = "add a_song a_url";
        ConsoleCommand cmd = new ConsoleCommand(cmdStr, TXID);
        final VoteRequest rawRequest = cmd.getVoteRequest();

        assertTrue(rawRequest instanceof AddRequest);
        AddRequest req = (AddRequest) rawRequest;
        assertEquals("a_song", req.getSongTuple().getName());
        assertEquals("a_url", req.getSongTuple().getUrl());
        assertEquals(1, req.getTransactionID());
    }

    @Test
    public void testUpdateRequest() throws Exception {
        String cmdStr = "update a_song a_song2 a_url";
        ConsoleCommand cmd = new ConsoleCommand(cmdStr, TXID);
        final VoteRequest rawRequest = cmd.getVoteRequest();

        assertTrue(rawRequest instanceof UpdateRequest);
        UpdateRequest req = (UpdateRequest) rawRequest;
        assertEquals("a_song", req.getSongName());
        assertEquals("a_song2", req.getUpdatedSong().getName());
        assertEquals("a_url", req.getUpdatedSong().getUrl());
        assertEquals(1, req.getTransactionID());
    }

    @Test
    public void testDeleteRequest() throws Exception {
        String cmdStr = "delete a_song";
        ConsoleCommand cmd = new ConsoleCommand(cmdStr, TXID);
        final VoteRequest rawRequest = cmd.getVoteRequest();

        assertTrue(rawRequest instanceof DeleteRequest);
        DeleteRequest req = (DeleteRequest) rawRequest;
        assertEquals("a_song", req.getSongName());
        assertEquals(1, req.getTransactionID());
    }

    @Test
    public void testPartialCommit() throws Exception {
        String cmdStr = "add a b -partialCommit 2";
        ConsoleCommand cmd = new ConsoleCommand(cmdStr, TXID);

        final List<Failure> failureModes = cmd.getFailureModes();
        assertEquals(1, failureModes.size());
        final Failure rawFailure = failureModes.get(0);
        assertTrue(rawFailure instanceof PartialBroadcast);
        final PartialBroadcast failure = (PartialBroadcast) rawFailure;
        assertEquals(Message.Command.COMMIT, failure.getStage());
        assertEquals(2, failure.getLastProcID());

        final VoteRequest rawRequest = cmd.getVoteRequest();
        assertTrue(rawRequest instanceof AddRequest);
        AddRequest req = (AddRequest) rawRequest;
        assertEquals("a", req.getSongTuple().getName());
        assertEquals("b", req.getSongTuple().getUrl());
        assertEquals(1, req.getTransactionID());
    }

    @Test
    public void testPartialCommitAndPartialPrecommit() throws Exception {
        String cmdStr = "add a b -partialCommit 2 -partialPrecommit 4";
        ConsoleCommand cmd = new ConsoleCommand(cmdStr, TXID);

        final List<Failure> failureModes = cmd.getFailureModes();
        assertEquals(2, failureModes.size());

        final Failure rawFailure = failureModes.get(0);
        assertTrue(rawFailure instanceof PartialBroadcast);
        final PartialBroadcast failure = (PartialBroadcast) rawFailure;
        assertEquals(Message.Command.COMMIT, failure.getStage());
        assertEquals(2, failure.getLastProcID());

        final Failure rawFailure2 = failureModes.get(1);
        assertTrue(rawFailure2 instanceof PartialBroadcast);
        final PartialBroadcast failure2 = (PartialBroadcast) rawFailure2;
        assertEquals(Message.Command.PRE_COMMIT, failure2.getStage());
        assertEquals(4, failure2.getLastProcID());

        final VoteRequest rawRequest = cmd.getVoteRequest();
        assertTrue(rawRequest instanceof AddRequest);
        AddRequest req = (AddRequest) rawRequest;
        assertEquals("a", req.getSongTuple().getName());
        assertEquals("b", req.getSongTuple().getUrl());
        assertEquals(1, req.getTransactionID());
    }

    @Test
    public void testDeathAfter() throws Exception {
        String cmdStr = "update a b c -deathAfter 2 5";
        ConsoleCommand cmd = new ConsoleCommand(cmdStr, TXID);

        final List<Failure> failureModes = cmd.getFailureModes();
        assertEquals(1, failureModes.size());
        final Failure rawFailure = failureModes.get(0);
        assertTrue(rawFailure instanceof DeathAfter);
        final DeathAfter failure = (DeathAfter) rawFailure;
        assertEquals(2, failure.getNumMsgs());
        assertEquals(5, failure.getProcID());
    }

    @Test
    public void testTwoDeathAfters() throws Exception {
        String cmdStr = "update a b c -deathAfter 2 5 -deathAfter 3 5";
        ConsoleCommand cmd = new ConsoleCommand(cmdStr, TXID);

        final List<Failure> failureModes = cmd.getFailureModes();
        assertEquals(2, failureModes.size());

        final Failure rawFailure = failureModes.get(0);
        assertTrue(rawFailure instanceof DeathAfter);
        final DeathAfter failure = (DeathAfter) rawFailure;
        assertEquals(2, failure.getNumMsgs());
        assertEquals(5, failure.getProcID());

        final Failure rawFailure2 = failureModes.get(1);
        assertTrue(rawFailure2 instanceof DeathAfter);
        final DeathAfter failure2 = (DeathAfter) rawFailure2;
        assertEquals(3, failure2.getNumMsgs());
        assertEquals(5, failure2.getProcID());
    }
}
