package node;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import system.network.Connection;
import messages.DecisionRequest;
import messages.Message;
import messages.vote_req.VoteRequest;
import node.base.Node;
import node.base.StateMachine;

public class ParticipantRecoveryStateMachine extends StateMachine {
	private enum ParticipantRecoveryState {
		WaitingForDecision,
	};
	
	private Node ownerNode;
	VoteRequest uncomitted;
	List<PeerReference> sortedPeers;
	int currentPeerIndex;
	Connection currentPeerConnection;
	ParticipantRecoveryState state;
	
	public ParticipantRecoveryStateMachine(Node ownerNode, VoteRequest uncommitted) {
		super();
		this.ownerNode = ownerNode;
		this.uncomitted = uncommitted;
		
		Collection<PeerReference> peerRefs = uncommitted.getPeerSet();
		sortedPeers = peerRefs.stream()
				.filter(peer -> peer.getNodeID() != ownerNode.getMyNodeID())
				.sorted((a, b) -> new Integer(a.getNodeID()).compareTo(b.getNodeID()))
				.collect(Collectors.toList());
		currentPeerIndex = 0;
		
		state = ParticipantRecoveryState.WaitingForDecision;
		PeerReference current = sortedPeers.get(currentPeerIndex);
        Connection currentPeerConnection = ownerNode.isConnectedTo(current)
                ? ownerNode.getPeerConnForId(current.nodeID)
                : ownerNode.connectTo(current);
        currentPeerConnection.sendMessage(new DecisionRequest(uncommitted.getTransactionID()));        
	}

	@Override
	public boolean receiveMessage(Connection overConnection) {
		Message message = overConnection.receiveMessage();
		if (message == null)
		{
			return false;
		}
		
		switch (message.getCommand()) {
		case COMMIT:
			if (state == ParticipantRecoveryState.WaitingForDecision) {
				ownerNode.commitAction(uncomitted);
				ownerNode.becomeParticipant();
			}
			break;
		case ABORT:
			if (state == ParticipantRecoveryState.WaitingForDecision) {
				ownerNode.becomeParticipant();
			}
			break;
		default:
			return false;
		}
		return true;
	}
}
