package node;

import messages.AbortRequest;
import messages.AckRequest;
import messages.CommitRequest;
import messages.DubCoordinatorMessage;
import messages.Message;
import messages.NoResponse;
import messages.PeerTimeout;
import messages.PrecommitRequest;
import messages.UncertainResponse;
import messages.YesResponse;
import messages.Message.Command;
import messages.vote_req.AddRequest;
import messages.vote_req.DeleteRequest;
import messages.vote_req.UpdateRequest;
import messages.vote_req.VoteRequest;
import node.CoordinatorStateMachine.CoordinatorState;
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
    SongTuple song, updatedSong;
    VoteRequest request;

    AddRequest add;
    UpdateRequest update;
    DeleteRequest delete;
    
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
        coordinatorPeerReferences.add(new PeerReference(TEST_COORD_ID, 0));
        for (int i = 0; i < 2; ++i) {
        	final int peerId = i + 2;
        	peerQueueSockets[i] = new QueueSocket(peerId, TEST_COORD_ID);
            QueueConnection coordinatorToPeer = peerQueueSockets[i].getConnectionToAID();
            syncNode.addConnection(coordinatorToPeer);
            coordinatorPeerReferences.add(new PeerReference(peerId, 0));
        }
        syncNode.setUpSet(coordinatorPeerReferences);
        
		song = new SongTuple("song", "url");
		updatedSong = new SongTuple("song", "updated");
		
		add = new AddRequest(song, TXID, coordinatorPeerReferences);
		update = new UpdateRequest("song", updatedSong, TXID, coordinatorPeerReferences);
		delete = new DeleteRequest("song", TXID, coordinatorPeerReferences);
    }

    private void receiveCommandFromTransactionManager(VoteRequest voteRequest) {
    	request = voteRequest;
        txnMgrToCoordinator.sendMessage(voteRequest);
		assertTrue(csm.receiveMessage(coordinatorToTxnMgr));
		assertEquals(CoordinatorStateMachine.CoordinatorState.WaitingForVotes, csm.getState());
    }
    
    private void receiveFailingCommandFromTransactionManager(VoteRequest voteRequest) {
        txnMgrToCoordinator.sendMessage(voteRequest);
		assertTrue(csm.receiveMessage(coordinatorToTxnMgr));
		assertEquals(CoordinatorStateMachine.CoordinatorState.WaitingForCommand, csm.getState());
    }
    
    private void peerRespondsWith(int peerIndex, Message response) {
		peerQueueSockets[peerIndex].getConnectionToBID().sendMessage(response);
		assertTrue(csm.receiveMessage(peerQueueSockets[peerIndex].getConnectionToAID()));
    }
    
    private void peerRespondsWithYes(int peerIndex) {
		peerRespondsWith(peerIndex, new YesResponse(request));
    }

    private void peerRespondsWithNo(int peerIndex) {
		peerRespondsWith(peerIndex, new NoResponse(request));
    }

    private void peerAcknowledgesPrecommit(int peerIndex) {
		peerQueueSockets[peerIndex].getConnectionToBID().sendMessage(new AckRequest(TXID));
		assertTrue(csm.receiveMessage(peerQueueSockets[peerIndex].getConnectionToAID()));
    }

    private void peerTimesOut(int peerIndex) {
		peerQueueSockets[peerIndex].getConnectionToBID().sendMessage(new PeerTimeout(peerIndex + 2));
		assertTrue(csm.receiveMessage(peerQueueSockets[peerIndex].getConnectionToAID()));
    }
    
    private Message getLastMesageToTxnMgr() {
    	return getLastMessageInQueue(coordinatorToTxnMgr.getOutQueue());
    }
    
    private Message getLastMessageToPeer(int peerIndex) {
    	return getLastMessageInQueue(peerQueueSockets[peerIndex].getConnectionToAID().getOutQueue());
    }

    private Message getLastMessageLogged() {
        Object[] messages = syncNode.getDtLog().getLoggedMessages().toArray();
        return (Message)messages[messages.length - 1];
    }

    private void startInTerminationProtocol(VoteRequest action) {
    	syncNode.becomeCoordinatorInRecovery(action);
    	csm = (CoordinatorStateMachine)syncNode.getStateMachine();
    }
    
	@Test
	public void testReceiveCommandFromTransactionManager_sendsVoteRequest() {
		receiveCommandFromTransactionManager(add);

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
		receiveCommandFromTransactionManager(add);
		peerRespondsWithYes(0);

		assertEquals(CoordinatorStateMachine.CoordinatorState.WaitingForVotes, csm.getState());
	}

	@Test
	public void testReceiveEnoughYesVotes_sendsPrecommit() {
		receiveCommandFromTransactionManager(add);
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
		receiveCommandFromTransactionManager(add);
		peerTimesOut(0);

		assertEquals(CoordinatorStateMachine.CoordinatorState.WaitingForCommand, csm.getState());
		Message abort = getLastMessageToPeer(1);
		assertEquals(Message.Command.ABORT, abort.getCommand());
		abort = getLastMesageToTxnMgr();
		assertEquals(Message.Command.ABORT, abort.getCommand());
		Message logged = getLastMessageLogged();
		assertTrue(logged instanceof AbortRequest);
	}

	@Test
	public void testReceiveNoVote_sendsAbortAndReturnsToInitialState() {
		receiveCommandFromTransactionManager(add);
		peerRespondsWithNo(0);

		assertEquals(CoordinatorStateMachine.CoordinatorState.WaitingForCommand, csm.getState());
		Message abort = getLastMessageToPeer(0);
		assertEquals(Message.Command.ABORT, abort.getCommand());
		abort = getLastMessageToPeer(1);
		assertEquals(Message.Command.ABORT, abort.getCommand());
		abort = getLastMesageToTxnMgr();
		assertEquals(Message.Command.ABORT, abort.getCommand());
		Message logged = getLastMessageLogged();
		assertTrue(logged instanceof AbortRequest);
	}

	@Test
	public void testReceiveNotEnoughAcks_stillWaitingForAcks() {
		receiveCommandFromTransactionManager(add);
		peerRespondsWithYes(0);
		peerRespondsWithYes(1);
		peerAcknowledgesPrecommit(0);

		assertEquals(CoordinatorStateMachine.CoordinatorState.WaitingForAcks, csm.getState());
	}

	@Test
	public void testReceiveEnoughAcks_sendsCommitAndReturnsToInitialState() {
		receiveCommandFromTransactionManager(add);
		peerRespondsWithYes(0);
		peerRespondsWithYes(1);
		peerAcknowledgesPrecommit(0);
		peerAcknowledgesPrecommit(1);

		assertEquals(CoordinatorStateMachine.CoordinatorState.WaitingForCommand, csm.getState());
		Message commit = getLastMessageToPeer(0);
		assertEquals(Message.Command.COMMIT, commit.getCommand());
		commit = getLastMessageToPeer(1);
		assertEquals(Message.Command.COMMIT, commit.getCommand());
		commit = getLastMesageToTxnMgr();
		assertEquals(Message.Command.COMMIT, commit.getCommand());
		Message logged = getLastMessageLogged();
		assertTrue(logged instanceof CommitRequest);
	}

	@Test
	public void testReceiveAckAndTimeout_sendsCommitAndReturnsToInitialState() {
		receiveCommandFromTransactionManager(add);
		peerRespondsWithYes(0);
		peerRespondsWithYes(1);
		peerAcknowledgesPrecommit(0);
		peerTimesOut(1);

		assertEquals(CoordinatorStateMachine.CoordinatorState.WaitingForCommand, csm.getState());
		Message commit = getLastMessageToPeer(0);
		assertEquals(Message.Command.COMMIT, commit.getCommand());
		commit = getLastMesageToTxnMgr();
		assertEquals(Message.Command.COMMIT, commit.getCommand());
		Message logged = getLastMessageLogged();
		assertTrue(logged instanceof CommitRequest);
	}

    @Test
    public void testUpdateStateOnCommit_addSong() throws Exception {
        receiveCommandFromTransactionManager(add);
        peerRespondsWithYes(0);
        peerRespondsWithYes(1);
        peerAcknowledgesPrecommit(0);
        peerAcknowledgesPrecommit(1);
        assertTrue(syncNode.hasExactSongTuple(song));
    }

    @Test
    public void testUpdateStateOnCommit_updateSong() throws Exception {
    	syncNode.addSong(song);
    	
        receiveCommandFromTransactionManager(update);
        peerRespondsWithYes(0);
        peerRespondsWithYes(1);
        peerAcknowledgesPrecommit(0);
        peerAcknowledgesPrecommit(1);
        assertTrue(syncNode.hasExactSongTuple(updatedSong));
    }

    @Test
    public void testUpdateStateOnCommit_deleteSong() throws Exception {
    	syncNode.addSong(song);
    	
        receiveCommandFromTransactionManager(delete);
        peerRespondsWithYes(0);
        peerRespondsWithYes(1);
        peerAcknowledgesPrecommit(0);
        peerAcknowledgesPrecommit(1);
        assertTrue(syncNode.hasNoSongs());
    }

    @Test
    public void testAbortRequestToAddExistingSong() throws Exception {
    	syncNode.addSong(song);
    	
    	receiveFailingCommandFromTransactionManager(add);
		Message abort = getLastMesageToTxnMgr();
		assertEquals(Message.Command.ABORT, abort.getCommand());
        assertTrue(syncNode.hasExactSongTuple(song));
    }

    @Test
    public void testAbortRequestToDeleteNonExistingSong() throws Exception {
    	receiveFailingCommandFromTransactionManager(delete);
		Message abort = getLastMesageToTxnMgr();
		assertEquals(Message.Command.ABORT, abort.getCommand());
        assertTrue(syncNode.hasNoSongs());
    }

    @Test
    public void testAbortRequestToUpdateNonExistingSong() throws Exception {
    	receiveFailingCommandFromTransactionManager(update);
		Message abort = getLastMesageToTxnMgr();
		assertEquals(Message.Command.ABORT, abort.getCommand());
        assertTrue(syncNode.hasNoSongs());
    }

    @Test
    public void testReceiveNOFromNodeAfterHavingAbortedTransaction() throws Exception {
		receiveCommandFromTransactionManager(add);
		peerRespondsWithNo(0);
		assertEquals(CoordinatorStateMachine.CoordinatorState.WaitingForCommand, csm.getState());
		
		peerRespondsWithNo(1);
		assertEquals(CoordinatorStateMachine.CoordinatorState.WaitingForCommand, csm.getState());
    }

    @Test
    public void testReceiveYESFromNodeAfterHavingAbortedTransaction() throws Exception {
		receiveCommandFromTransactionManager(add);
		peerRespondsWithNo(0);
		assertEquals(CoordinatorStateMachine.CoordinatorState.WaitingForCommand, csm.getState());
		
		peerRespondsWithYes(1);
		assertEquals(CoordinatorStateMachine.CoordinatorState.WaitingForCommand, csm.getState());
    }
    
    @Test
    public void startCoordinatorInRecovery_peerSendsCommit_sendCommitToEveryoneElse() {
    	startInTerminationProtocol(add);
    	assertEquals(CoordinatorState.WaitingForStates, csm.getState());
    	peerRespondsWith(0, new CommitRequest(TXID));
    	
    	Message last = getLastMessageToPeer(0);
    	assertEquals(Command.STATE_REQUEST, last.getCommand());	// nothing was sent to 0 after the state_req
    	last = getLastMessageToPeer(1);
    	assertEquals(Command.COMMIT, last.getCommand());
    	last = getLastMesageToTxnMgr();
    	assertEquals(Command.COMMIT, last.getCommand());
    	assertEquals(CoordinatorState.WaitingForCommand, csm.getState());
    }

    @Test
    public void startCoordinatorInRecovery_firstPeerSendsCommitThenSecondSendsUncertain_noFurtherEffect() {
    	startInTerminationProtocol(add);
    	assertEquals(CoordinatorState.WaitingForStates, csm.getState());
    	peerRespondsWith(0, new CommitRequest(TXID));
    	assertEquals(CoordinatorState.WaitingForCommand, csm.getState());
    	
    	peerRespondsWith(1, new UncertainResponse(TXID));
    	assertEquals(CoordinatorState.WaitingForCommand, csm.getState());
    }
    
    @Test
    public void startCoordinatorInRecovery_peerSendsAbort_sendAbortToEveryoneElse() {
    	startInTerminationProtocol(add);
    	assertEquals(CoordinatorState.WaitingForStates, csm.getState());
    	peerRespondsWith(0, new AbortRequest(TXID));
    	
    	Message last = getLastMessageToPeer(0);
    	assertEquals(Command.STATE_REQUEST, last.getCommand());	// nothing was sent to 0 after the state_req
    	last = getLastMessageToPeer(1);
    	assertEquals(Command.ABORT, last.getCommand());
    	last = getLastMesageToTxnMgr();
    	assertEquals(Command.ABORT, last.getCommand());
    	assertEquals(CoordinatorState.WaitingForCommand, csm.getState());
    }
    
    @Test
    public void startCoordinatorInRecovery_firstPeerSendsAbortThenSecondSendsUncertain_noFurtherEffect() {
    	startInTerminationProtocol(add);
    	assertEquals(CoordinatorState.WaitingForStates, csm.getState());
    	peerRespondsWith(0, new CommitRequest(TXID));
    	assertEquals(CoordinatorState.WaitingForCommand, csm.getState());
    	
    	peerRespondsWith(1, new UncertainResponse(TXID));
    	assertEquals(CoordinatorState.WaitingForCommand, csm.getState());
    }
    
    @Test
    public void starCoordinatortInRecovery_onePeerSendsPrecommitAndOtherSendsUncertain_handledAsPrecommit() {
    	startInTerminationProtocol(add);
    	assertEquals(CoordinatorState.WaitingForStates, csm.getState());
    	peerRespondsWith(0, new PrecommitRequest(TXID));
    	assertEquals(CoordinatorState.WaitingForStates, csm.getState());
    	peerRespondsWith(1, new UncertainResponse(TXID));
    	
    	Message last = getLastMessageToPeer(0);
    	assertEquals(Command.PRE_COMMIT, last.getCommand());
    	last = getLastMessageToPeer(1);
    	assertEquals(Command.PRE_COMMIT, last.getCommand());
    	assertEquals(CoordinatorState.WaitingForAcks, csm.getState());
    }
}
