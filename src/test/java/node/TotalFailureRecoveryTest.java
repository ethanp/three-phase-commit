package node;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import messages.PeerTimeout;
import messages.YesResponse;
import messages.vote_req.AddRequest;
import node.system.SyncNode;
import org.junit.Before;
import org.junit.Test;
import system.network.QueueSocket;
import util.SongTuple;
import util.TestCommon;

public class TotalFailureRecoveryTest extends TestCommon {
	
    SyncNode[] nodesUnderTest;
    QueueSocket queueSocket;
    QueueSocket[] peerQueueSockets;
    ArrayList<PeerReference> peerReferences;
    SongTuple songTuple;
    ParticipantRecoveryStateMachine prsm;
    
    @Before
    public void setUp() throws Exception {
        nodesUnderTest = new SyncNode[3]; 
        peerReferences = new ArrayList<PeerReference>();
        for (int i = 0; i < 3; ++i) {
        	final int peerId = i + 1;
        	nodesUnderTest[i] = new SyncNode(peerId, null);	
            peerReferences.add(new PeerReference(peerId, 0));
        }
               
        songTuple = new SongTuple("song", "url");
    }
       
	@Test
	public void testTotalFailureRecovery_lastToRecoverBecomesCoordinator() {		
		AddRequest add = new AddRequest(songTuple, TXID, peerReferences);
		YesResponse yes = new YesResponse(add);
		PeerTimeout firstTimeout = new PeerTimeout(1);
		PeerTimeout secondTimeout = new PeerTimeout(2);
		SyncNode firstNode = nodesUnderTest[0];
		SyncNode secondNode = nodesUnderTest[1];
		SyncNode thirdNode = nodesUnderTest[2];
        QueueSocket oneToTwo = new QueueSocket(1, 2);
        QueueSocket oneToThree = new QueueSocket(1, 3);
        QueueSocket twoToThree = new QueueSocket(2, 3);

		// set up first log
		SyncNode stubNode = new SyncNode(-1, null);
		stubNode.logMessage(add);
		stubNode.logMessage(yes);
			
		// first node "connects" to second and third, but they're not up yet 
		firstNode.addConnection(oneToTwo.getConnectionToBID());
		firstNode.addConnection(oneToThree.getConnectionToBID());        

		firstNode.setDtLog(stubNode.getDtLog());
		firstNode.recoverFromDtLog();
		ParticipantRecoveryStateMachine firstSM = (ParticipantRecoveryStateMachine)firstNode.getStateMachine();
		// first node sends decision_req to second node, which times out
		oneToTwo.getConnectionToBID().getOutQueue().clear();	// remove the message so it isn't received later
		oneToTwo.getConnectionToAID().sendMessage(new PeerTimeout(2));
		assertTrue(firstSM.receiveMessage(oneToTwo.getConnectionToBID()));
		// first node sends decision_req to third node, which times out
		oneToThree.getConnectionToBID().getOutQueue().clear(); // remove the message so it isn't received later
		oneToThree.getConnectionToAID().sendMessage(new PeerTimeout(3));
		assertTrue(firstSM.receiveMessage(oneToThree.getConnectionToBID()));
		// first node is now waiting again and has sent another decision_req to second node, which times outs 
		oneToTwo.getConnectionToBID().getOutQueue().clear();	// remove the message so it isn't received later
		// assert its up set
		Collection<Integer> firstUpSetIntersection = firstSM.getUpSetIntersection();
		assertEquals(3, firstUpSetIntersection.size());
		assertTrue(firstUpSetIntersection.contains(1));
		assertTrue(firstUpSetIntersection.contains(2));
		assertTrue(firstUpSetIntersection.contains(3));
        // verify first node's recovery set
		Collection<Integer> firstRecoveredSet = firstSM.getRecoveredProcesses();
		assertEquals(1, firstRecoveredSet.size());
		assertTrue(firstRecoveredSet.contains(1));
		
		// set up second log
		stubNode = new SyncNode(-1, null);
		stubNode.logMessage(add);
		stubNode.logMessage(yes);
		stubNode.logMessage(firstTimeout);

		// second node connects to first and third, but third isn't up yet
		secondNode.addConnection(oneToTwo.getConnectionToAID());        
		secondNode.addConnection(twoToThree.getConnectionToBID());
        
        secondNode.setDtLog(stubNode.getDtLog());
        secondNode.recoverFromDtLog();
		ParticipantRecoveryStateMachine secondSM = (ParticipantRecoveryStateMachine)secondNode.getStateMachine();
        // second node sends decision_req to first node, and first node replies with in_recovery
		assertTrue(firstSM.receiveMessage(oneToTwo.getConnectionToBID()));
		assertTrue(secondSM.receiveMessage(oneToTwo.getConnectionToAID()));
        // verify second node's recovery set
		Collection<Integer> secondRecoveredSet = secondSM.getRecoveredProcesses();
		assertEquals(2, secondRecoveredSet.size());
		assertTrue(secondRecoveredSet.contains(1));
		assertTrue(secondRecoveredSet.contains(2));		
        // second node sends decision_req to third node, which times out
        twoToThree.getConnectionToBID().getInQueue().clear();	// remove the message so it isn't received later
        twoToThree.getConnectionToAID().sendMessage(new PeerTimeout(3));
		assertTrue(secondNode.getStateMachine().receiveMessage(twoToThree.getConnectionToBID()));
        // second node is now waiting for a coordinator; check its up set
		Collection<Integer> secondUpSetIntersection = secondSM.getUpSetIntersection();
		assertEquals(2, secondUpSetIntersection.size());
		assertTrue(secondUpSetIntersection.contains(2));
		assertTrue(secondUpSetIntersection.contains(3));
		
		// set up third log
		stubNode = new SyncNode(-1, null);
		stubNode.logMessage(add);
		stubNode.logMessage(yes);
		stubNode.logMessage(firstTimeout);
		stubNode.logMessage(secondTimeout);
	
		// third node connects to first and second
		thirdNode.addConnection(twoToThree.getConnectionToAID());
		thirdNode.addConnection(oneToThree.getConnectionToAID());
        
        thirdNode.setDtLog(stubNode.getDtLog());
        thirdNode.recoverFromDtLog();
        // third node sends decision_req to first node, and first node replies with in_recovery
		assertTrue(firstNode.getStateMachine().receiveMessage(oneToThree.getConnectionToBID()));
		assertTrue(thirdNode.getStateMachine().receiveMessage(oneToThree.getConnectionToAID()));
        // third node sends decision_req to second node, and second node replies with in_recovery
		assertTrue(secondNode.getStateMachine().receiveMessage(twoToThree.getConnectionToBID()));
		assertTrue(thirdNode.getStateMachine().receiveMessage(twoToThree.getConnectionToAID()));
        // third node now sends ur_elected to first node, and changes to participant recovery state
		fail("todo");
		// first node is now in coordinator termination protocol, and sends state_request to second and third nodes
		// second node is still waiting for a coordinator
	}
}
