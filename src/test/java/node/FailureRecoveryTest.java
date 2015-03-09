package node;

import messages.DecisionRequest;
import messages.ElectedMessage;
import messages.Message;
import messages.Message.Command;
import messages.PeerTimeout;
import messages.YesResponse;
import messages.vote_req.AddRequest;
import node.base.StateMachine;
import node.system.SyncNode;
import org.junit.Before;
import org.junit.Test;
import system.network.QueueConnection;
import system.network.QueueSocket;
import util.Common;
import util.SongTuple;
import util.TestCommon;

import java.util.ArrayList;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FailureRecoveryTest extends TestCommon {

    SyncNode[] nodesUnderTest;
    QueueSocket[] peerQueueSockets;
    ArrayList<PeerReference> peerReferences;
    SongTuple songTuple;
    ParticipantRecoveryStateMachine prsm;
    QueueSocket txnMgrQueueSocket;
    QueueSocket[] txnMgrQueueSockets;
    QueueConnection txnMgrToCoordinator;
    QueueConnection coordinatorToTxnMgr;

    @Before
    public void setUp() throws Exception {
        nodesUnderTest = new SyncNode[3];
        peerReferences = new ArrayList<PeerReference>();
        txnMgrQueueSocket = new QueueSocket(1, Common.TXN_MGR_ID);
        txnMgrQueueSockets = new QueueSocket[3];
        coordinatorToTxnMgr = txnMgrQueueSocket.getConnectionToBID();
        for (int i = 0; i < 3; ++i) {
            QueueSocket socket = new QueueSocket(i+1, Common.TXN_MGR_ID);
            txnMgrQueueSockets[i] = socket;
            final int peerId = i+1;
            nodesUnderTest[i] = new SyncNode(peerId, socket.getConnectionToBID());
            peerReferences.add(new PeerReference(peerId, 0));
        }

        songTuple = new SongTuple("song", "url");
    }

	@Test
	public void testTotalFailureRecovery_lastToRecoverBecomesCoordinator() {
		final AddRequest add = new AddRequest(songTuple, TXID, peerReferences);
		final YesResponse yes = new YesResponse(add);
		final PeerTimeout firstTimeout = new PeerTimeout(1);
		final PeerTimeout secondTimeout = new PeerTimeout(2);
		final SyncNode firstNode = nodesUnderTest[0];
		final SyncNode secondNode = nodesUnderTest[1];
		final SyncNode thirdNode = nodesUnderTest[2];
        final QueueSocket oneToTwoSocket = new QueueSocket(1, 2);
        final QueueSocket oneToThreeSocket = new QueueSocket(1, 3);
        final QueueSocket twoToThreeSocket = new QueueSocket(2, 3);
        final QueueConnection oneToTwo = oneToTwoSocket.getConnectionToBID();
        final QueueConnection twoToOne = oneToTwoSocket.getConnectionToAID();
        final QueueConnection oneToThree = oneToThreeSocket.getConnectionToBID();
        final QueueConnection threeToOne = oneToThreeSocket.getConnectionToAID();
        final QueueConnection twoToThree = twoToThreeSocket.getConnectionToBID();
        final QueueConnection threeToTwo = twoToThreeSocket.getConnectionToAID();

		// set up first log
		SyncNode stubNode = new SyncNode(-1, null);
		stubNode.logMessage(add);
		stubNode.logMessage(yes);

		// first node "connects" to second and third, but they're not up yet
		firstNode.addConnection(oneToTwo);
        firstNode.addConnection(oneToThree);

		firstNode.setDtLog(stubNode.getDtLog());
		firstNode.recoverFromDtLog();
		ParticipantRecoveryStateMachine firstSM = (ParticipantRecoveryStateMachine)firstNode.getStateMachine();

		// first node sends decision_req to second node, which times out
		oneToTwo.getOutQueue().clear(); // remove the message so it isn't received later

        twoToOne.sendMessage(new PeerTimeout(2));
		assertTrue(firstSM.receiveMessage(oneToTwo));

		// first node sends decision_req to third node, which times out
		oneToThree.getOutQueue().clear(); // remove the message so it isn't received later

        threeToOne.sendMessage(new PeerTimeout(3));
		assertTrue(firstSM.receiveMessage(oneToThree));

		// first node is now waiting again and has sent another decision_req to second node, which times outs
		oneToTwo.getOutQueue().clear();	// remove the message so it isn't received later

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

		// second node connects to first and third, although third isn't up yet
		secondNode.addConnection(twoToOne);
        secondNode.addConnection(twoToThree);

        secondNode.setDtLog(stubNode.getDtLog());
        secondNode.recoverFromDtLog();

        ParticipantRecoveryStateMachine secondSM = (ParticipantRecoveryStateMachine)secondNode.getStateMachine();
        secondSM.sendDecisionRequestToCurrentPeer();

        // second node sends decision_req to first node
		assertTrue(firstSM.receiveMessage(oneToTwo));

        // first node replies with in_recovery
		assertTrue(secondSM.receiveMessage(twoToOne));

        // verify second node's recovery set
		Collection<Integer> secondRecoveredSet = secondSM.getRecoveredProcesses();
		assertEquals(2, secondRecoveredSet.size());
		assertTrue(secondRecoveredSet.contains(1));
		assertTrue(secondRecoveredSet.contains(2));

        // second node sends decision_req to third node, which times out
        twoToThree.getOutQueue().clear();	// remove the message so it isn't received later
        threeToTwo.sendMessage(new PeerTimeout(3));
		assertTrue(secondNode.getStateMachine().receiveMessage(twoToThree));

		// second node sends decision_req to first node
        assertTrue(twoToOne.getOutQueue().peek() instanceof DecisionRequest);
		twoToOne.getOutQueue().clear();	// remove the message so it isn't received later

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
		thirdNode.addConnection(threeToTwo);
		thirdNode.addConnection(threeToOne);

        thirdNode.setDtLog(stubNode.getDtLog());
        thirdNode.recoverFromDtLog();

        ((ParticipantRecoveryStateMachine)thirdNode.getStateMachine()).sendDecisionRequestToCurrentPeer();

        // third node sends decision_req to first node, and first node replies with in_recovery
		assertTrue(firstNode.getStateMachine().receiveMessage(oneToThree));
		assertTrue(thirdNode.getStateMachine().receiveMessage(threeToOne));

        // third node sends decision_req to second node, and second node replies with in_recovery
		assertTrue(secondNode.getStateMachine().receiveMessage(twoToThree));
		assertTrue(thirdNode.getStateMachine().receiveMessage(threeToTwo));

        // third node now sends ur_elected to first node, and changes to participant recovery state
		assertTrue(firstNode.getStateMachine().receiveMessage(oneToThree));

		// first node is now in coordinator termination protocol, and sends state_request to second and third nodes
		assertTrue(firstNode.getStateMachine() instanceof CoordinatorStateMachine);
		assertTrue(secondNode.getStateMachine().receiveMessage(twoToOne));
		assertTrue(thirdNode.getStateMachine().receiveMessage(threeToOne));

		// second and third nodes are now in participant recovery mode
		assertTrue(secondNode.getStateMachine() instanceof ParticipantStateMachine);
		assertTrue(thirdNode.getStateMachine() instanceof ParticipantStateMachine);

		// second node sends Uncertain to first node
		assertTrue(firstNode.getStateMachine().receiveMessage(oneToTwo));

		// third node sends Uncertain to first node
		assertTrue(firstNode.getStateMachine().receiveMessage(oneToThree));

		// first node aborts; sends abort to transaction manager, second and third nodes
		Message lastToSecond = getLastMessageInQueue(oneToTwo.getOutQueue());
		assertEquals(Command.ABORT, lastToSecond.getCommand());
		Message lastToThird = getLastMessageInQueue(oneToThree.getOutQueue());
		assertEquals(Command.ABORT, lastToThird.getCommand());
		Message lastToTxnMgr = getLastMessageInQueue(txnMgrQueueSockets[0].getConnectionToBID().getOutQueue());
		assertEquals(Command.ABORT, lastToTxnMgr.getCommand());
	}

    @Test
    public void testPartialPrecommit() throws Exception {
        AddRequest add = new AddRequest(songTuple, TXID, peerReferences);
		YesResponse yes = new YesResponse(add);
        ElectedMessage ur_el = new ElectedMessage(TXID);
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

		// first node "connects" to second and third
		firstNode.addConnection(oneToTwo.getConnectionToBID());
		firstNode.addConnection(oneToThree.getConnectionToBID());

        secondNode.addConnection(oneToTwo.getConnectionToAID());
        thirdNode.addConnection(oneToThree.getConnectionToAID());

        final QueueConnection rcv2Frm3 = twoToThree.getConnectionToBID();
        final QueueConnection rcv3Frm2 = twoToThree.getConnectionToAID();

        final QueueConnection rcv2Frm1 = oneToTwo.getConnectionToAID();
        final QueueConnection rcv1Frm2 = oneToTwo.getConnectionToBID();

        secondNode.addConnection(rcv2Frm3);
        thirdNode.addConnection(rcv3Frm2);

        ParticipantStateMachine psm2 = (ParticipantStateMachine)secondNode.getStateMachine();
        psm2.setAction(add);
        psm2.setOngoingTransactionID(TXID);
        psm2.setPrecommitted(true);
        psm2.setCoordinatorID(1);
        Collection<PeerReference> upset = new ArrayList<>();
        upset.add(new PeerReference(1, 1));
        upset.add(new PeerReference(2, 2));
        upset.add(new PeerReference(3, 3));
        secondNode.setUpSet(upset);


        ParticipantStateMachine psm3 = (ParticipantStateMachine)thirdNode.getStateMachine();
        psm3.setAction(add);
        psm3.setOngoingTransactionID(TXID);
        psm3.setPrecommitted(false);
        psm3.setCoordinatorID(1);
        thirdNode.setUpSet(upset);



        rcv2Frm3.sendMessage(firstTimeout);

        // receive the timeout
        assertTrue(psm3.receiveMessage(rcv3Frm2));

        // receive ur-elected
        assertTrue(psm2.receiveMessage(rcv2Frm3));
        assertTrue(secondNode.getStateMachine() instanceof CoordinatorStateMachine);

        StateMachine sm2 = secondNode.getStateMachine();

        // receive state-req
        assertTrue(psm3.receiveMessage(rcv3Frm2));

        // receive uncertain
        assertTrue(sm2.receiveMessage(rcv2Frm3));

        // receive pre-commit
        assertTrue(psm3.receiveMessage(rcv3Frm2));

        // receive ack
        assertTrue(sm2.receiveMessage(rcv2Frm3));

        // receive commit
        assertTrue(psm3.receiveMessage(rcv3Frm2));

        // they both committed
        assertTrue(secondNode.getDecisionFor(TXID).getCommand() == Command.COMMIT);
        assertTrue(thirdNode.getDecisionFor(TXID).getCommand() == Command.COMMIT);

        // node 1 reboots
        firstNode.setDtLog(stubNode.getDtLog());
        firstNode.recoverFromDtLog();
        ((ParticipantRecoveryStateMachine)firstNode.getStateMachine()).sendDecisionRequestToCurrentPeer();

        // receive dec-req
        assertTrue(sm2.receiveMessage(rcv2Frm1));

        StateMachine sm1 = firstNode.getStateMachine();

        // receive commit
        assertTrue(sm1.receiveMessage(rcv1Frm2));
    }
}
