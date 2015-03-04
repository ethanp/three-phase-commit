package node;

import messages.Message;
import messages.vote_req.AddRequest;
import messages.vote_req.DeleteRequest;
import messages.vote_req.UpdateRequest;
import messages.vote_req.VoteRequest;
import node.base.DTLog;
import node.base.Node;

public class LogRecoveryStateMachine {
	
	private VoteRequest currentRequest;
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
		case ACK:
		case DUB_COORDINATOR:
		case NO:
		case PRE_COMMIT:
		case TIMEOUT:
		case UR_ELECTED:
			break;
		default:
			break;		
		}
	}
}
