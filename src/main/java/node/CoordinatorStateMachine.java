package node;

import messages.AbortRequest;
import messages.CommitRequest;
import messages.DelayMessage;
import messages.Message;
import messages.PeerTimeout;
import messages.PrecommitRequest;
import messages.vote_req.VoteRequest;
import node.base.Node;
import node.base.StateMachine;
import system.network.Connection;
import util.Common;

import java.io.EOFException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;

import static util.Common.NO_ONGOING_TRANSACTION;

/**
 * Ethan Petuchowski 2/28/15
 */
public class CoordinatorStateMachine extends StateMachine {
	public enum CoordinatorState {
		WaitingForCommand,
		WaitingForVotes,
		WaitingForAcks,
	}

	private CoordinatorState state;
	private Collection<Connection> txnConnections;
	private Node ownerNode;
	private VoteRequest action;
	private int yesVotes;
	private int ongoingTransactionID;
	private int acks;

	public CoordinatorStateMachine(Node ownerNode) {
		this.ownerNode = ownerNode;
		resetToWaiting();
	}

	public CoordinatorState getState() {
		return state;
	}

    @Override public boolean receiveMessage(Connection overConnection) {

        Message message;

        try { message = overConnection.receiveMessage(); }
        catch (EOFException e) { message = null; }

        if (message == null) {
    		return false;
    	}

        try { Thread.sleep(Common.MESSAGE_DELAY); }
        catch (InterruptedException ignored) {}

        synchronized (this) {
            System.out.println("Coordinator "+ownerNode.getMyNodeID()+" "+
                               "received a "+message.getCommand());

            switch (message.getCommand()) {
                case ADD:
                case UPDATE:
                case DELETE:
                    if (state == CoordinatorState.WaitingForCommand) {
                        receivePlaylistCommand((VoteRequest) message);
                        break;
                    }
                    else
                        throw new RuntimeException("Received command when not waiting for one");
                case YES:
                    ownerNode.cancelTimersFor(overConnection.getReceiverID());
                    if (state == CoordinatorState.WaitingForVotes) {
                        ++yesVotes;
                        checkForEnoughYesVotes();
                    }
                    break;
                case NO:
                    ownerNode.cancelTimersFor(overConnection.getReceiverID());
                    if (state == CoordinatorState.WaitingForVotes) {
                        abortCurrentTransaction();
                    }
                    break;
                case ACK:
                    ownerNode.cancelTimersFor(overConnection.getReceiverID());
                    if (state == CoordinatorState.WaitingForAcks) {
                        ++acks;
                        checkForEnoughAcks();
                    }
                    else
                        throw new RuntimeException("Received ACK when not waiting for one");
                    break;
                case TIMEOUT:
                    int peerID = ((PeerTimeout) message).getPeerId();
                    ownerNode.cancelTimersFor(peerID);
                    PeerReference ref = getPeerSet().stream()
                                                    .filter(pr -> pr.nodeID == peerID)
                                                    .findFirst()
                                                    .get();
                    onTimeout(ref);
                    break;

                // TODO decision request
                case DECISION_REQUEST:
                    break;

                /* fail cases */
                case PARTIAL_BROADCAST:
                case DEATH_AFTER:
                    ownerNode.addFailure(message);
                    break;

                case DELAY:
                    Common.MESSAGE_DELAY = ((DelayMessage) message).getDelaySec()*1000;
                    break;

                default:
                    throw new RuntimeException("invalid message for coordinator");
            }
        }
        return true;
    }

    private void onTimeout(PeerReference peerReference) {

        /* inform mgr of timeout */
        ownerNode.sendTxnMgrMsg(new PeerTimeout(peerReference.getNodeID()));

    	// if we're waiting for a command, a timeout doesn't matter.
    	if (state == CoordinatorState.WaitingForVotes) {
    		getPeerSet().remove(peerReference);
    		txnConnections.remove(ownerNode.getPeerConnForId(peerReference.nodeID));
    		abortCurrentTransaction();
    	}
    	else if (state == CoordinatorState.WaitingForAcks) {
    		getPeerSet().remove(peerReference);
    		txnConnections.remove(ownerNode.getPeerConnForId(peerReference.nodeID));
    		checkForEnoughAcks();
    	}
    }

    private void resetToWaiting() {
		ongoingTransactionID = NO_ONGOING_TRANSACTION;
		setPeerSet(null);
		txnConnections = null;
		state = CoordinatorState.WaitingForCommand;
    }

    private void receivePlaylistCommand(VoteRequest message) {

    	if (ownerNode.getVoteValue(message))
    	{
	        // send vote requests to all peers
	        final Collection<PeerReference> peerSet =
	                message.getPeerSet().stream()
	                       .filter(ref -> ref.getNodeID() != ownerNode.getMyNodeID())
	                       .collect(Collectors.toList());

	        Collection<Connection> conns = new ArrayList<>();
	    	ownerNode.logMessage(message);
	        /* connect to every peer the ownerNode is not already connected to */
	        for (PeerReference reference : peerSet) {

	            Connection conn = ownerNode.isConnectedTo(reference)
	                              ? ownerNode.getPeerConnForId(reference.nodeID)
	                              : ownerNode.connectTo(reference);

	            conns.add(conn);
                ownerNode.send(conn, message);

                /* start timers on everyone */
                ownerNode.resetTimersFor(reference.getNodeID());
	        }
	        setPeerSet(peerSet);
	        txnConnections = conns;
	        ownerNode.getPeerConns().addAll(txnConnections);

	        state = CoordinatorState.WaitingForVotes;
	        action = message;
	        ongoingTransactionID = message.getTransactionID();
	        yesVotes = 0;
	        acks = 0;
    	}
    	else
    	{
    		ownerNode.logMessage(message);
    		AbortRequest abort = new AbortRequest(message.getTransactionID());
    		ownerNode.logMessage(abort);
    		ownerNode.sendTxnMgrMsg(abort);
    	}
    }

    private void abortCurrentTransaction() {
		AbortRequest abort = new AbortRequest(ongoingTransactionID);
		ownerNode.logMessage(abort);
        ownerNode.broadcast(txnConnections, abort);
		resetToWaiting();
    }

    private void checkForEnoughYesVotes() {
    	final Collection<PeerReference> peerSet = getPeerSet();
    	if (yesVotes >= peerSet.size()) {
    		PrecommitRequest precommit = new PrecommitRequest(ongoingTransactionID);
    		for (Connection connection : txnConnections) {
                ownerNode.resetTimersFor(connection.getReceiverID());
                ownerNode.send(connection, precommit);
    		}
    		state = CoordinatorState.WaitingForAcks;
    	}
    }

    private void checkForEnoughAcks() {
    	final Collection<PeerReference> peerSet = getPeerSet();
		if (acks >= peerSet.size()) {
			CommitRequest commit = new CommitRequest(ongoingTransactionID);
			ownerNode.logMessage(commit);
            for (Connection connection : txnConnections) {
                ownerNode.send(connection, commit);
			}
            ownerNode.sendTxnMgrMsg(commit);
            ownerNode.commitAction(action);
            resetToWaiting();
		}
    }
}
