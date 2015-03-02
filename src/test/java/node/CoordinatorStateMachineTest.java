package node;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Queue;

import messages.AckRequest;
import messages.Message;
import messages.NoResponse;
import messages.YesResponse;
import messages.Message.Command;
import messages.vote_req.AddRequest;
import messages.vote_req.VoteRequest;
import node.system.SyncNode;

import org.junit.Before;
import org.junit.Test;

import system.network.QueueConnection;
import system.network.QueueSocket;
import util.Common;
import util.SongTuple;
import util.TestCommon;

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
        syncNode = new SyncNode(TEST_COORD_ID, null);
        txnMgrQueueSocket = new QueueSocket(TEST_COORD_ID, Common.TXN_MGR_ID);
        txnMgrToCoordinator = txnMgrQueueSocket.getConnectionToAID();
        coordinatorToTxnMgr = txnMgrQueueSocket.getConnectionToBID();
        syncNode.addConnection(coordinatorToTxnMgr);
        
        txnMgrToCoordinator.sendMessage(new Message(Command.DUB_COORDINATOR, -1));
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
    
    private Message getLastMessageToPeer(int peerIndex) {
    	Message message;
    	Queue<Message> queue = peerQueueSockets[peerIndex].getConnectionToBID().getInQueue();
    	do {
    		message = queue.poll();
    	} while (queue.size() > 0);
    	return message;
    }
    
	@Test
	public void testReceiveCommandFromTransactionManager_sendsVoteRequest() {
		receiveCommandFromTransactionManager();
		
		assertEquals(CoordinatorStateMachine.CoordinatorState.WaitingForVotes, csm.getState());
		Message voteReq = getLastMessageToPeer(0);
		assertEquals(request, voteReq);
		voteReq = getLastMessageToPeer(1);
		assertEquals(request, voteReq);
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
		csm.onTimeout(coordinatorPeerReferences.get(0));
		
		assertEquals(CoordinatorStateMachine.CoordinatorState.WaitingForCommand, csm.getState());
		Message abort = getLastMessageToPeer(1);
		assertEquals(Message.Command.ABORT, abort.getCommand());
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
	}	
	
	@Test
	public void testReceiveAckAndTimeout_sendsCommitAndReturnsToInitialState() {
		receiveCommandFromTransactionManager();
		peerRespondsWithYes(0);
		peerRespondsWithYes(1);
		peerAcknowledgesPrecommit(0);
		csm.onTimeout(coordinatorPeerReferences.get(1));

		assertEquals(CoordinatorStateMachine.CoordinatorState.WaitingForCommand, csm.getState());
		Message commit = getLastMessageToPeer(0);
		assertEquals(Message.Command.COMMIT, commit.getCommand());
	}
}
