package node;

import messages.AbortRequest;
import messages.CommitRequest;
import messages.Message;
import messages.PrecommitRequest;
import messages.vote_req.VoteRequest;
import node.base.Node;
import node.base.StateMachine;
import system.network.Connection;

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
    	Message message = overConnection.receiveMessage();
    	if (message == null) {
    		return false;
    	}

        switch (message.getCommand()) {
	        case ADD:
            case UPDATE:
            case DELETE:
            	if (state == CoordinatorState.WaitingForCommand) {
            		receivePlaylistCommand((VoteRequest)message);
            		break;
            	}
            	else
            		throw new RuntimeException("Received command when not waiting for one");
            case YES:
            	if (state == CoordinatorState.WaitingForVotes) {
	            	++yesVotes;
	            	checkForEnoughYesVotes();
            	}
            	else
            		throw new RuntimeException("Received Yes vote when not waiting for one");
                break;
            case NO:
            	if (state == CoordinatorState.WaitingForVotes) {
            		abortCurrentTransaction();
            	}
            	else
            		throw new RuntimeException("Received No vote when not waiting for one");
                break;
            case ACK:
            	if (state == CoordinatorState.WaitingForAcks) {
            		++acks;
            		checkForEnoughAcks();
            	}
            	else
            		throw new RuntimeException("Received ACK when not waiting for one");
                break;

            default:
                throw new RuntimeException("invalid message for coordinator");
        }
        return true;
    }

    public void onTimeout(PeerReference peerReference) {
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

        // send vote requests to all peers
        final Collection<PeerReference> peerSet =
                message.getPeerSet().stream()
                       .filter(ref -> ref.getNodeID() != ownerNode.getMyNodeID())
                       .collect(Collectors.toList());

        Collection<Connection> conns = new ArrayList<>();

        /* connect to every peer the ownerNode is not already connected to */
        for (PeerReference reference : peerSet) {

            Connection conn = ownerNode.isConnectedTo(reference)
                              ? ownerNode.getPeerConnForId(reference.nodeID)
                              : ownerNode.connectTo(reference);

            conns.add(conn);
            conn.sendMessage(message);
        }
        setPeerSet(peerSet);
        txnConnections = conns;

        state = CoordinatorState.WaitingForVotes;
        ongoingTransactionID = message.getTransactionID();
        yesVotes = 0;
        acks = 0;
    }

    private void abortCurrentTransaction() {
		AbortRequest abort = new AbortRequest(ongoingTransactionID);
		for (Connection connection : txnConnections) {
			connection.sendMessage(abort);
		}
		resetToWaiting();
    }

    private void checkForEnoughYesVotes() {
    	final Collection<PeerReference> peerSet = getPeerSet();
    	if (yesVotes >= peerSet.size()) {
    		PrecommitRequest precommit = new PrecommitRequest(ongoingTransactionID);
    		for (Connection connection : txnConnections) {
    			connection.sendMessage(precommit);
    		}
    		state = CoordinatorState.WaitingForAcks;
    	}
    }

    private void checkForEnoughAcks() {
    	final Collection<PeerReference> peerSet = getPeerSet();
		if (acks >= peerSet.size()) {
			CommitRequest commit = new CommitRequest(ongoingTransactionID);
			for (Connection connection : txnConnections) {
				connection.sendMessage(commit);
			}
            ownerNode.sendTxnMgrMsg(commit);
            resetToWaiting();
		}
    }
}
