package node;

import messages.AbortRequest;
import messages.AckRequest;
import messages.CommitRequest;
import messages.DubCoordinatorMessage;
import messages.Message;
import messages.NoResponse;
import messages.PeerTimeout;
import messages.YesResponse;
import messages.vote_req.AddRequest;
import messages.vote_req.VoteRequest;
import node.system.SyncNode;
import org.junit.Before;
import org.junit.Test;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import system.network.QueueConnection;
import system.network.QueueSocket;
import util.Common;
import util.SongTuple;
import util.TestCommon;

import java.util.ArrayList;
import java.util.Queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CoordinatorStateMachineTest extends TestCommon {
    SyncNode syncNode;
    CoordinatorStateMachine csm;
    QueueSocket[] peerQueueSockets;
    QueueSocket txnMgrQueueSocket;
    QueueConnection txnMgrToCoordinator;
    QueueConnection coordinatorToTxnMgr;
    ArrayList<PeerReference> coordinatorPeerReferences;
    SongTuple song;
    VoteRequest request;

    @Before
    public void setUp() throws Exception {
        /* connect a syncNode to this test setup */
        txnMgrQueueSocket = new QueueSocket(TEST_COORD_ID, Common.TXN_MGR_ID);
        txnMgrToCoordinator = txnMgrQueueSocket.getConnectionToAID();
        coordinatorToTxnMgr = txnMgrQueueSocket.getConnectionToBID();
        syncNode = new SyncNode(TEST_COORD_ID, coordinatorToTxnMgr);

        /* Dub it a Coordinator */
        txnMgrToCoordinator.sendMessage(new DubCoordinatorMessage());
        syncNode.getStateMachine().receiveMessage(coordinatorToTxnMgr);

        csm = (CoordinatorStateMachine)syncNode.getStateMachine();

        peerQueueSockets = new QueueSocket[2];
        coordinatorPeerReferences = new ArrayList<PeerReference>();
        for (int i = 0; i < 2; ++i) {
        	final int peerId = i + 2;
        	peerQueueSockets[i] = new QueueSocket(peerId, TEST_COORD_ID);
            QueueConnection coordinatorToPeer = peerQueueSockets[i].getConnectionToAID();
            syncNode.addConnection(coordinatorToPeer);
            coordinatorPeerReferences.add(new PeerReference(peerId, 0));
        }
    }

    private void receiveCommandFromTransactionManager() {
		song = new SongTuple("song", "url");
		request = new AddRequest(song, TXID, coordinatorPeerReferences);
        txnMgrToCoordinator.sendMessage(request);
		assertTrue(csm.receiveMessage(coordinatorToTxnMgr));
		assertEquals(CoordinatorStateMachine.CoordinatorState.WaitingForVotes, csm.getState());
    }

    private void peerRespondsWithYes(int peerIndex) {
		peerQueueSockets[peerIndex].getConnectionToBID().sendMessage(new YesResponse(request));
		assertTrue(csm.receiveMessage(peerQueueSockets[peerIndex].getConnectionToAID()));
    }

    private void peerRespondsWithNo(int peerIndex) {
		peerQueueSockets[peerIndex].getConnectionToBID().sendMessage(new NoResponse(request));
		assertTrue(csm.receiveMessage(peerQueueSockets[peerIndex].getConnectionToAID()));
    }

    private void peerAcknowledgesPrecommit(int peerIndex) {
		peerQueueSockets[peerIndex].getConnectionToBID().sendMessage(new AckRequest(TXID));
		assertTrue(csm.receiveMessage(peerQueueSockets[peerIndex].getConnectionToAID()));
    }

    private void peerTimesOut(int peerIndex) {
		peerQueueSockets[peerIndex].getConnectionToBID().sendMessage(new PeerTimeout(peerIndex + 2));
		assertTrue(csm.receiveMessage(peerQueueSockets[peerIndex].getConnectionToAID()));
    }

    private Message getLastMessageToPeer(int peerIndex) {
    	Message message;
    	Queue<Message> queue = peerQueueSockets[peerIndex].getConnectionToAID().getOutQueue();
    	do {
    		message = queue.poll();
    	} while (queue.size() > 0);
    	return message;
    }

    private Message getLastMessageLogged() {
        Object[] messages = syncNode.getDtLog().getLoggedMessages().toArray();
        return (Message)messages[messages.length - 1];
    }

	@Test
	public void testReceiveCommandFromTransactionManager_sendsVoteRequest() {
		receiveCommandFromTransactionManager();

		assertEquals(CoordinatorStateMachine.CoordinatorState.WaitingForVotes, csm.getState());
        Message voteReq = getLastMessageToPeer(0);
        assertEquals(request, voteReq);
		voteReq = getLastMessageToPeer(1);
		assertEquals(request, voteReq);
		Message logged = getLastMessageLogged();
		assertTrue(logged instanceof AddRequest);
	}

	@Test
	public void testReceiveNotEnoughVotes_stillWaitingForVotes() {
		receiveCommandFromTransactionManager();
		peerRespondsWithYes(0);

		assertEquals(CoordinatorStateMachine.CoordinatorState.WaitingForVotes, csm.getState());
	}

	@Test
	public void testReceiveEnoughYesVotes_sendsPrecommit() {
		receiveCommandFromTransactionManager();
		peerRespondsWithYes(0);
		peerRespondsWithYes(1);

		assertEquals(CoordinatorStateMachine.CoordinatorState.WaitingForAcks, csm.getState());
		Message precommit = getLastMessageToPeer(0);
		assertEquals(Message.Command.PRE_COMMIT, precommit.getCommand());
		precommit = getLastMessageToPeer(1);
		assertEquals(Message.Command.PRE_COMMIT, precommit.getCommand());
	}

	@Test
	public void testTimeoutWhileWaitingForVotes_sendsAbortAndReturnsToInitialState() {
		receiveCommandFromTransactionManager();
		peerTimesOut(0);

		assertEquals(CoordinatorStateMachine.CoordinatorState.WaitingForCommand, csm.getState());
		Message abort = getLastMessageToPeer(1);
		assertEquals(Message.Command.ABORT, abort.getCommand());
		Message logged = getLastMessageLogged();
		assertTrue(logged instanceof AbortRequest);
	}

	@Test
	public void testReceiveNoVote_sendsAbortAndReturnsToInitialState() {
		receiveCommandFromTransactionManager();
		peerRespondsWithNo(0);

		assertEquals(CoordinatorStateMachine.CoordinatorState.WaitingForCommand, csm.getState());
		Message abort = getLastMessageToPeer(0);
		assertEquals(Message.Command.ABORT, abort.getCommand());
		abort = getLastMessageToPeer(1);
		assertEquals(Message.Command.ABORT, abort.getCommand());
		Message logged = getLastMessageLogged();
		assertTrue(logged instanceof AbortRequest);
	}

	@Test
	public void testReceiveNotEnoughAcks_stillWaitingForAcks() {
		receiveCommandFromTransactionManager();
		peerRespondsWithYes(0);
		peerRespondsWithYes(1);
		peerAcknowledgesPrecommit(0);

		assertEquals(CoordinatorStateMachine.CoordinatorState.WaitingForAcks, csm.getState());
	}

	@Test
	public void testReceiveEnoughAcks_sendsCommitAndReturnsToInitialState() {
		receiveCommandFromTransactionManager();
		peerRespondsWithYes(0);
		peerRespondsWithYes(1);
		peerAcknowledgesPrecommit(0);
		peerAcknowledgesPrecommit(1);

		assertEquals(CoordinatorStateMachine.CoordinatorState.WaitingForCommand, csm.getState());
		Message commit = getLastMessageToPeer(0);
		assertEquals(Message.Command.COMMIT, commit.getCommand());
		commit = getLastMessageToPeer(1);
		assertEquals(Message.Command.COMMIT, commit.getCommand());
		Message logged = getLastMessageLogged();
		assertTrue(logged instanceof CommitRequest);
	}

	@Test
	public void testReceiveAckAndTimeout_sendsCommitAndReturnsToInitialState() {
		receiveCommandFromTransactionManager();
		peerRespondsWithYes(0);
		peerRespondsWithYes(1);
		peerAcknowledgesPrecommit(0);
		peerTimesOut(1);

		assertEquals(CoordinatorStateMachine.CoordinatorState.WaitingForCommand, csm.getState());
		Message commit = getLastMessageToPeer(0);
		assertEquals(Message.Command.COMMIT, commit.getCommand());
		Message logged = getLastMessageLogged();
		assertTrue(logged instanceof CommitRequest);
	}

    @Test
    public void testReceiveEnoughVotesButVoteNo_sendsAbortAndReturnsToInitialState() throws Exception {
        // TODO node already has song to add in its state
        receiveCommandFromTransactionManager();
        peerRespondsWithYes(0);
        peerRespondsWithYes(1);
        throw new NotImplementedException();
    }

    @Test
    public void testSendsCommitToTxnMgrToo() throws Exception {
        throw new NotImplementedException();
    }

    @Test
    public void testSendsAbortToTxnMgrToo() throws Exception {
        throw new NotImplementedException();
    }

    @Test
    public void testUpdateStateOnCommit_addSong() throws Exception {
        receiveCommandFromTransactionManager();
        // TODO the following
        assertTrue(syncNode.hasExactSongTuple(A_SONG_TUPLE));
    }

    @Test
    public void testUpdateStateOnCommit_updateSong() throws Exception {
        throw new NotImplementedException();
    }

    @Test
    public void testUpdateStateOnCommit_deleteSong() throws Exception {
        throw new NotImplementedException();
    }

    @Test
    public void testAbortRequestToAddExistingSong() throws Exception {
        throw new NotImplementedException();
    }

    @Test
    public void testAbortRequestToDeleteNonExistingSong() throws Exception {
        throw new NotImplementedException();
    }

    @Test
    public void testAbortRequestToUpdateNonExistingSong() throws Exception {
        throw new NotImplementedException();
    }
}
