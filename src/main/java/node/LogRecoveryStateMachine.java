package node;

import messages.Message;
import messages.vote_req.AddRequest;
import messages.vote_req.DeleteRequest;
import messages.vote_req.UpdateRequest;
import messages.vote_req.VoteRequest;
import node.base.DTLog;
import node.base.Node;

public class LogRecoveryStateMachine {
	public static VoteRequest recoverLog(Node node) {
		LogRecoveryStateMachine machine = new LogRecoveryStateMachine(node);
		for (Message message : node.getDtLog().getLoggedMessages()) {
			machine.handleLoggedMessage(message);
		}
		return machine.currentRequest;
	}
	
	private VoteRequest currentRequest;
	private Node node;
	
	private LogRecoveryStateMachine(Node node) {
		this.node = node;
	}
	
	private void handleLoggedMessage(Message message) {
		switch (message.getCommand()) {
		case ADD:
		case DELETE:
		case UPDATE:
			if (currentRequest == null) {
				currentRequest = (VoteRequest)message;
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
		case ACK:
		case DUB_COORDINATOR:
		case NO:
		case PRE_COMMIT:
		case TIMEOUT:
		case UR_ELECTED:
		case YES:
			break;
		default:
			break;		
		}
	}
}
