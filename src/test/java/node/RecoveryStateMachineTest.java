package node;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;

import messages.AbortRequest;
import messages.CommitRequest;
import messages.InRecoveryResponse;
import messages.Message;
import messages.PeerTimeout;
import messages.PrecommitRequest;
import messages.UncertainResponse;
import messages.YesResponse;
import messages.Message.Command;
import messages.vote_req.AddRequest;
import messages.vote_req.DeleteRequest;
import messages.vote_req.UpdateRequest;
import node.mock.ByteArrayDTLog;
import node.system.SyncNode;

import org.junit.Before;
import org.junit.Test;

import system.network.QueueConnection;
import system.network.QueueSocket;
import util.SongTuple;
import util.TestCommon;

public class RecoveryStateMachineTest extends TestCommon {
	
    SyncNode nodeUnderTest;
    QueueSocket[] peerQueueSockets;
    ArrayList<PeerReference> peerReferences;
    SongTuple songTuple, updated;
    ParticipantRecoveryStateMachine prsm;
    
    @Before
    public void setUp() throws Exception {
        nodeUnderTest = new SyncNode(TEST_PEER_ID, null);
        
        peerQueueSockets = new QueueSocket[3];
        peerReferences = new ArrayList<PeerReference>();
        for (int i = 0; i < 3; ++i) {
        	final int peerId = i + 2;
        	if (i != 0) {
	        	peerQueueSockets[i] = new QueueSocket(peerId, TEST_PEER_ID);
	            QueueConnection toOtherPeer = peerQueueSockets[i].getConnectionToAID();
	            nodeUnderTest.addConnection(toOtherPeer);
        	}
            peerReferences.add(new PeerReference(peerId, 0));
        }
        
        songTuple = new SongTuple("song", "url");
        updated = new SongTuple("song", "updated");
    }
    
	@Test
	public void node_recoverFromDtLog_logContainsCommittedAdd_recoversWithSongAndReadyToParticipate() {		
		SyncNode stubNode = new SyncNode(TEST_PEER_ID, null);
		
        AddRequest add = new AddRequest(songTuple, TXID, peerReferences);
        stubNode.logMessage(add);
        stubNode.logMessage(new YesResponse(add));
        stubNode.logMessage(new CommitRequest(TXID));		
		ByteArrayDTLog mockLog = (ByteArrayDTLog)stubNode.getDtLog();
		
		nodeUnderTest.setDtLog(mockLog);
		nodeUnderTest.recoverFromDtLog();
		
		assertTrue(nodeUnderTest.hasExactSongTuple(songTuple));
		assertTrue(nodeUnderTest.getStateMachine() instanceof ParticipantStateMachine);
	}
	
	@Test
	public void node_recoverFromDtLog_logContainsAbortedAdd_recoversWithSongAndReadyToParticipate() {		
		SyncNode stubNode = new SyncNode(TEST_PEER_ID, null);
		
        AddRequest add = new AddRequest(songTuple, TXID, peerReferences);
        stubNode.logMessage(add);
        stubNode.logMessage(new YesResponse(add));
        stubNode.logMessage(new AbortRequest(TXID));		
		ByteArrayDTLog mockLog = (ByteArrayDTLog)stubNode.getDtLog();
		
		nodeUnderTest.setDtLog(mockLog);
		nodeUnderTest.recoverFromDtLog();
		
		assertTrue(nodeUnderTest.hasNoSongs());
		assertTrue(nodeUnderTest.getStateMachine() instanceof ParticipantStateMachine);
	}
	
	@Test
	public void node_recoverFromDtLog_logContainsCommittedAddAndUpdate_recoversWithUpdatedSongAndReadyToParticipate() {		
		SyncNode stubNode = new SyncNode(TEST_PEER_ID, null);
		
        AddRequest add = new AddRequest(songTuple, TXID, peerReferences);
        stubNode.logMessage(add);
        stubNode.logMessage(new YesResponse(add));
        stubNode.logMessage(new CommitRequest(TXID));
        UpdateRequest update = new UpdateRequest("song", updated, TXID+1, peerReferences);
        stubNode.logMessage(update);
        stubNode.logMessage(new YesResponse(update));
        stubNode.logMessage(new CommitRequest(TXID+1));
        
		ByteArrayDTLog mockLog = (ByteArrayDTLog)stubNode.getDtLog();		
		nodeUnderTest.setDtLog(mockLog);
		nodeUnderTest.recoverFromDtLog();
		
		assertTrue(nodeUnderTest.hasExactSongTuple(updated));
		assertTrue(nodeUnderTest.getStateMachine() instanceof ParticipantStateMachine);
	}
		
	@Test
	public void node_recoverFromDtLog_logContainsCommittedAddAndAbortedUpdate_recoversWithOriginalSongAndReadyToParticipate() {		
		SyncNode stubNode = new SyncNode(TEST_PEER_ID, null);
		
        AddRequest add = new AddRequest(songTuple, TXID, peerReferences);
        stubNode.logMessage(add);
        stubNode.logMessage(new YesResponse(add));
        stubNode.logMessage(new CommitRequest(TXID));
        UpdateRequest update = new UpdateRequest("song", updated, TXID+1, peerReferences);
        stubNode.logMessage(update);
        stubNode.logMessage(new YesResponse(update));
        stubNode.logMessage(new AbortRequest(TXID+1));
        
		ByteArrayDTLog mockLog = (ByteArrayDTLog)stubNode.getDtLog();		
		nodeUnderTest.setDtLog(mockLog);
		nodeUnderTest.recoverFromDtLog();
		
		assertTrue(nodeUnderTest.hasExactSongTuple(songTuple));
		assertTrue(nodeUnderTest.getStateMachine() instanceof ParticipantStateMachine);
	}
	
	@Test
	public void node_recoverFromDtLog_logContainsCommittedAddAndDelete_recoversWithoutSongAndReadyToParticipate() {		
		SyncNode stubNode = new SyncNode(TEST_PEER_ID, null);
		
        AddRequest add = new AddRequest(songTuple, TXID, peerReferences);
        stubNode.logMessage(add);
        stubNode.logMessage(new YesResponse(add));
        stubNode.logMessage(new CommitRequest(TXID));
        DeleteRequest delete = new DeleteRequest("song", TXID+1, peerReferences);
        stubNode.logMessage(delete);
        stubNode.logMessage(new YesResponse(delete));
        stubNode.logMessage(new CommitRequest(TXID+1));
        
		ByteArrayDTLog mockLog = (ByteArrayDTLog)stubNode.getDtLog();		
		nodeUnderTest.setDtLog(mockLog);
		nodeUnderTest.recoverFromDtLog();
		
		assertTrue(nodeUnderTest.hasNoSongs());
		assertTrue(nodeUnderTest.getStateMachine() instanceof ParticipantStateMachine);
	}
	
	@Test
	public void node_recoverFromDtLog_logContainsCommittedAddAndAbortedDelete_recoversWithSongAndReadyToParticipate() {		
		SyncNode stubNode = new SyncNode(TEST_PEER_ID, null);
		
        AddRequest add = new AddRequest(songTuple, TXID, peerReferences);
        stubNode.logMessage(add);
        stubNode.logMessage(new YesResponse(add));
        stubNode.logMessage(new CommitRequest(TXID));
        DeleteRequest delete = new DeleteRequest("song", TXID+1, peerReferences);
        stubNode.logMessage(delete);
        stubNode.logMessage(new YesResponse(delete));
        stubNode.logMessage(new AbortRequest(TXID+1));
        
		ByteArrayDTLog mockLog = (ByteArrayDTLog)stubNode.getDtLog();		
		nodeUnderTest.setDtLog(mockLog);
		nodeUnderTest.recoverFromDtLog();
		
		assertTrue(nodeUnderTest.hasExactSongTuple(songTuple));
		assertTrue(nodeUnderTest.getStateMachine() instanceof ParticipantStateMachine);
	}
	
	private void setupLogWithUncommittedAdd() {
		SyncNode stubNode = new SyncNode(TEST_PEER_ID, null);
        AddRequest add = new AddRequest(songTuple, TXID, peerReferences);
        stubNode.logMessage(add);
        stubNode.logMessage(new YesResponse(add));
		nodeUnderTest.setDtLog(stubNode.getDtLog());
		nodeUnderTest.recoverFromDtLog();		
		prsm = (ParticipantRecoveryStateMachine)nodeUnderTest.getStateMachine();
		assertTrue(prsm != null);
	}
	
    private void peerRespondsWith(int peerIndex, Message response) {
		peerQueueSockets[peerIndex].getConnectionToBID().sendMessage(response);
		assertTrue(prsm.receiveMessage(peerQueueSockets[peerIndex].getConnectionToAID()));
    }    
    
    private Message getLastMessageToPeer(int peerIndex) {
    	return getLastMessageInQueue(peerQueueSockets[peerIndex].getConnectionToAID().getOutQueue());
    }
    
	@Test
	public void node_recoverFromUncommittedRequest_upSetIntersectionContainsPeersFromRequest() {
		setupLogWithUncommittedAdd();
        Collection<Integer> upSet = prsm.getUpSetIntersection();
        assertEquals(3, upSet.size());
        assertTrue(upSet.contains(2));
        assertTrue(upSet.contains(3));
        assertTrue(upSet.contains(4));
	}
	
	@Test
	public void node_recoverFromUncommittedRequest_sendsDecisionRequestToFirstPeer() {
		setupLogWithUncommittedAdd();
        Message lastToPeer = getLastMessageToPeer(1);
        assertEquals(Message.Command.DECISION_REQUEST, lastToPeer.getCommand());
	}
	
	@Test
	public void node_recoverFromUncommittedRequest_receivesCommitDecision_followsDecision() {		
		setupLogWithUncommittedAdd();		
		peerRespondsWith(1, new CommitRequest(TXID));
        assertTrue(nodeUnderTest.hasExactSongTuple(songTuple));
        assertTrue("node should now be a participant", nodeUnderTest.getStateMachine() instanceof ParticipantStateMachine);
	}	
	
	@Test
	public void node_recoverFromUncommittedRequest_receivesAbortDecision_followsDecision() {		
		setupLogWithUncommittedAdd();		
		peerRespondsWith(1, new AbortRequest(TXID));
        assertTrue(nodeUnderTest.hasNoSongs());
        assertTrue("node should now be a participant", nodeUnderTest.getStateMachine() instanceof ParticipantStateMachine);
	}	
	
	@Test
	public void node_recoverFromUncommittedRequest_firstPeerUncertain_asksSecondPeer() {		
		setupLogWithUncommittedAdd();		
		peerRespondsWith(1, new UncertainResponse(TXID));
		Message lastToSecondPeer = getLastMessageToPeer(2);
		assertEquals(Command.DECISION_REQUEST, lastToSecondPeer.getCommand());
	}
	
	@Test
	public void node_recoverFromUncommittedRequest_firstPeerPrecommitted_asksSecondPeer() {		
		setupLogWithUncommittedAdd();		
		peerRespondsWith(1, new PrecommitRequest(TXID));
		Message lastToSecondPeer = getLastMessageToPeer(2);
		assertEquals(Command.DECISION_REQUEST, lastToSecondPeer.getCommand());
	}	
	
	@Test
	public void node_recoverFromUncommittedRequest_firstPeerTimesOut_asksSecondPeer() {		
		setupLogWithUncommittedAdd();		
		peerRespondsWith(1, new PeerTimeout(3));
		Message lastToSecondPeer = getLastMessageToPeer(2);
		assertEquals(Command.DECISION_REQUEST, lastToSecondPeer.getCommand());
	}		
	
	@Test
	public void node_recoverFromUncommittedRequest_firstPeerUncertainAndSecondSendsAbort_followsDecision() {		
		setupLogWithUncommittedAdd();		
		peerRespondsWith(1, new UncertainResponse(TXID));
		peerRespondsWith(2, new AbortRequest(TXID));
        assertTrue(nodeUnderTest.hasNoSongs());
        assertTrue("node should now be a participant", nodeUnderTest.getStateMachine() instanceof ParticipantStateMachine);
	}
	
	@Test
	public void node_recoverFromUncommittedRequest_allPeersUncertain_triesFirstPeerAgain() {		
		setupLogWithUncommittedAdd();
		Message lastToFirstPeer = getLastMessageToPeer(1);
		peerRespondsWith(1, new UncertainResponse(TXID));
		peerRespondsWith(2, new UncertainResponse(TXID));
		// NOTE: we're not checking the waiting part of this
		lastToFirstPeer = getLastMessageToPeer(1);
		assertEquals(Command.DECISION_REQUEST, lastToFirstPeer.getCommand());
	}
	
	@Test
	public void node_recoverFromUncommittedRequest_firstPeerInRecovery_updatesUpSetAndAsksSecondPeer() {		
		setupLogWithUncommittedAdd();
		ArrayList<Integer> peerUpSet = new ArrayList<Integer>();
		peerUpSet.add(3);
		peerUpSet.add(4);
		
		peerRespondsWith(1, new InRecoveryResponse(TXID, peerUpSet));
		Message lastToSecondPeer = getLastMessageToPeer(2);
		assertEquals(Command.DECISION_REQUEST, lastToSecondPeer.getCommand());
		
		Collection<Integer> updatedUpSet = prsm.getUpSetIntersection();
		assertEquals(2, updatedUpSet.size());
		assertTrue(updatedUpSet.contains(3));
		assertTrue(updatedUpSet.contains(4));
	}
	
	@Test
	public void node_recoverFromUncommittedRequest_firstPeerInRecoveryAndSecondSendsAbort_followsDecision() {		
		setupLogWithUncommittedAdd();		
		peerRespondsWith(1, new InRecoveryResponse(TXID, new ArrayList<Integer>()));
		peerRespondsWith(2, new AbortRequest(TXID));
        assertTrue(nodeUnderTest.hasNoSongs());
        assertTrue("node should now be a participant", nodeUnderTest.getStateMachine() instanceof ParticipantStateMachine);
	}
	
	// remaining tests: put an add request and yes vote with no commit in log, then recover to get node into recovery state machine
	
	// test that when node receives recovering status from all peers and the last to fail is up, it elects a leader and goes to participant recovery mode
	// test that when node receives recovering status from all peers and the last to fail is not up:
		// after subsequent decision requests, it sends status
		// after receiving state request, it sends vote and goes into participant recovery mode
}
