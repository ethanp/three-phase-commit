package node;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import system.network.Connection;
import messages.DecisionRequest;
import messages.InRecoveryResponse;
import messages.Message;
import messages.vote_req.VoteRequest;
import node.base.Node;
import node.base.StateMachine;

public class ParticipantRecoveryStateMachine extends StateMachine {
	private enum ParticipantRecoveryState {
		NoInformation,
		SomeProcessesUncertain,
		SomeProcessesInRecovery
	};
	
	private Node ownerNode;
	VoteRequest uncommitted;
	List<PeerReference> sortedPeers;
	int currentPeerIndex;
	Connection currentPeerConnection;
	ParticipantRecoveryState state;
	private Collection<Integer> recoveredProcesses;
	private Collection<Integer> upSetIntersection;
	private Collection<Integer> originalUpSet;
	
	public Collection<Integer> getRecoveredProcesses() {		
		return recoveredProcesses;
	}
	
	public Collection<Integer> getUpSetIntersection() {
		return upSetIntersection;
	}
	
	public ParticipantRecoveryStateMachine(Node ownerNode, VoteRequest uncommitted, Collection<PeerReference> lastUpSet) {
		super();
		this.ownerNode = ownerNode;
		this.uncommitted = uncommitted;
		
		Collection<PeerReference> peerRefsInRequest = uncommitted.getPeerSet();
		sortedPeers = peerRefsInRequest.stream()
				.filter(peer -> peer.getNodeID() != ownerNode.getMyNodeID())
				.sorted((a, b) -> new Integer(a.getNodeID()).compareTo(b.getNodeID()))
				.collect(Collectors.toList());
		
		originalUpSet = new ArrayList<Integer>();
		for (PeerReference peerRef : lastUpSet) {
			originalUpSet.add(peerRef.getNodeID());			
		}
		
		resetToNoInformation();
		sendDecisionRequestToCurrentPeer();
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
			if (state == ParticipantRecoveryState.NoInformation) {
				ownerNode.commitAction(uncommitted);
				ownerNode.becomeParticipant();
			}
			else
				throw new RuntimeException("Received commit response in a state where we weren't expecting one.");
			break;
		case ABORT:
			if (state == ParticipantRecoveryState.NoInformation ||
				state == ParticipantRecoveryState.SomeProcessesUncertain ||
				state == ParticipantRecoveryState.SomeProcessesInRecovery) {
				ownerNode.becomeParticipant();
			}
			break;
		case UNCERTAIN:
		case PRE_COMMIT:
			if (state == ParticipantRecoveryState.NoInformation ||
				state == ParticipantRecoveryState.SomeProcessesUncertain ||
				state == ParticipantRecoveryState.SomeProcessesInRecovery) {
				
				state = ParticipantRecoveryState.SomeProcessesUncertain;
				advanceToNextProcessOrRewind();
				
			}
			break;
		case TIMEOUT:
			advanceToNextProcessOrRewind();
			break;
		case IN_RECOVERY:
			if (state == ParticipantRecoveryState.NoInformation ||
				state == ParticipantRecoveryState.SomeProcessesInRecovery) {
				
				state = ParticipantRecoveryState.SomeProcessesInRecovery;
				InRecoveryResponse inRecovery = (InRecoveryResponse)message;
				// add to recovered processes
				this.recoveredProcesses.add(sortedPeers.get(currentPeerIndex).getNodeID());
				// compute new UP set intersection
				upSetIntersection.removeIf(peerID -> !inRecovery.getLastUpSet().contains(peerID));
				
				if (++currentPeerIndex < sortedPeers.size()) {
					sendDecisionRequestToCurrentPeer();
				}
				else {
					// all peers are in recovery, so see if we can elect a leader.
					// TODO
					throw new RuntimeException();
				}
			}
			else if (state == ParticipantRecoveryState.SomeProcessesUncertain) {
				// recovering process doesn't matter; keep walking the list
				advanceToNextProcessOrRewind();
			}
			break;
		case DECISION_REQUEST:
			overConnection.sendMessage(new InRecoveryResponse(uncommitted.getTransactionID(), originalUpSet));
			break;
		default:
			return false;
		}
		return true;
	}
	
	private void resetToNoInformation() {
		state = ParticipantRecoveryState.NoInformation;
		currentPeerIndex = 0;
		recoveredProcesses = new ArrayList<Integer>();
		recoveredProcesses.add(ownerNode.getMyNodeID());
		upSetIntersection = new ArrayList<Integer>();
		for (Integer peerId : originalUpSet) {
			upSetIntersection.add(peerId);
		}
	}
	
	private void sendDecisionRequestToCurrentPeer() {
		PeerReference current = sortedPeers.get(currentPeerIndex);
        Connection currentPeerConnection = ownerNode.isConnectedTo(current)
                ? ownerNode.getPeerConnForId(current.nodeID)
                : ownerNode.connectTo(current);
        currentPeerConnection.sendMessage(new DecisionRequest(uncommitted.getTransactionID()));        
	}
	
	private void advanceToNextProcessOrRewind() {
		if (++currentPeerIndex < sortedPeers.size()) {
			sendDecisionRequestToCurrentPeer();
		}
		else {
			// wait and try again
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			resetToNoInformation();
			sendDecisionRequestToCurrentPeer();			
		}
	}
}
