package node;

import static org.junit.Assert.*;

import java.util.ArrayList;

import messages.AbortRequest;
import messages.CommitRequest;
import messages.YesResponse;
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
    //ParticipantStateMachine participantSM;
    QueueSocket queueSocket;
    QueueConnection peerToCoordinator;
    QueueConnection coordinatorToPeer;
    ArrayList<PeerReference> peerReferences;
    SongTuple songTuple, updated;
    
    @Before
    public void setUp() throws Exception {
        nodeUnderTest = new SyncNode(TEST_PEER_ID, null);
        //participantSM = (ParticipantStateMachine) nodeUnderTest.getStateMachine();
        queueSocket = new QueueSocket(TEST_COORD_ID, TEST_PEER_ID);
        peerToCoordinator = queueSocket.getConnectionToAID();
        nodeUnderTest.addConnection(peerToCoordinator);
        coordinatorToPeer = queueSocket.getConnectionToBID();
        
        peerReferences = new ArrayList<PeerReference>();
        for (int i = 0; i < 2; ++i) {
        	final int peerId = i + 2;
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
}
