package node;

import java.util.Collection;

import messages.Message;
import messages.PeerTimeout;
import messages.vote_req.AddRequest;
import messages.vote_req.DeleteRequest;
import messages.vote_req.UpdateRequest;
import messages.vote_req.VoteRequest;
import node.base.DTLog;
import node.base.Node;

public class LogRecoveryStateMachine {
	
	private VoteRequest currentRequest;
	private Collection<PeerReference> lastUpSet;
	private Node node;
	private boolean votedYes;
	
	public LogRecoveryStateMachine(Node node) {
		this.node = node;
		for (Message message : node.getDtLog().getLoggedMessages()) {
			handleLoggedMessage(message);
		}
	}
	
	public VoteRequest getUncommittedRequest() {
		return currentRequest;
	}
	
	public Collection<PeerReference> getLastUpSet() {
		return lastUpSet;
	}
	
	public boolean didVoteYesOnRequest() {
		return votedYes;
	}
	
	private void handleLoggedMessage(Message message) {
		switch (message.getCommand()) {
		case ADD:
		case DELETE:
		case UPDATE:
			if (currentRequest == null) {
				currentRequest = (VoteRequest)message;
				lastUpSet = ((VoteRequest)message).getCloneOfPeerSet();
				votedYes = false;
			}
			else
				throw new RuntimeException("Shouldn't have seen a vote req without committing previous one.");
			break;
		case ABORT:
			if (currentRequest != null) {
				currentRequest = null;
			}
			else
				throw new RuntimeException("Shouldn't have seen an abort without starting a vote req.");
			break;
		case COMMIT:
			if (currentRequest != null) {
				node.commitAction(currentRequest);
				currentRequest = null;
			}
			else
				throw new RuntimeException("Shouldn't have seen a commit without starting a vote req.");
			break;
		case YES:
			if (currentRequest != null) {
				votedYes = true;
			}
			break;
		case TIMEOUT:
			if (currentRequest != null) {
				PeerTimeout timeout = (PeerTimeout)message;
				lastUpSet.removeIf(ref -> ref.getNodeID() == timeout.getPeerId());
			}
			break;
		case ACK:
		case DUB_COORDINATOR:
		case NO:
		case PRE_COMMIT:
		case UR_ELECTED:
			break;
		default:
			break;		
		}
	}
}
