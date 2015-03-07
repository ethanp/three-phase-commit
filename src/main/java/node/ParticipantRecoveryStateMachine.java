package node;

import messages.DecisionRequest;
import messages.DelayMessage;
import messages.ElectedMessage;
import messages.InRecoveryResponse;
import messages.Message;
import messages.UncertainResponse;
import messages.YesResponse;
import messages.Message.Command;
import messages.vote_req.VoteRequest;
import node.base.Node;
import node.base.StateMachine;
import system.network.Connection;
import util.Common;

import java.io.EOFException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.swing.OverlayLayout;

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
	private Set<Integer> recoveredProcesses;
	private Set<Integer> upSetIntersection;
	private Set<Integer> originalUpSet;

	public Set<Integer> getRecoveredProcesses() {
		return recoveredProcesses;
	}

	public Set<Integer> getUpSetIntersection() {
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

		originalUpSet = new HashSet<Integer>();
		for (PeerReference peerRef : lastUpSet) {
			originalUpSet.add(peerRef.getNodeID());
		}

		resetToNoInformation();
		sendDecisionRequestToCurrentPeer();
	}

	@Override
	public boolean receiveMessage(Connection overConnection) {
        Message message = null;
        try {
            message = overConnection.receiveMessage();
        }

        catch (EOFException e) {
            System.err.println("Node "+ownerNode.getMyNodeID()+" received EOFException from "+overConnection.getReceiverID());
            System.exit(Common.EXIT_FAILURE);
            message = null;
        }

        if (message == null)
		{
			return false;
		}

        int receiverID = overConnection.getReceiverID();
        if (!message.getCommand().equals(Command.TIMEOUT)) {
			recoveredProcesses.add(receiverID);
        }
        else {
        	recoveredProcesses.remove(receiverID);
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
				upSetIntersection.retainAll(inRecovery.getLastUpSet());

				if (++currentPeerIndex < sortedPeers.size()) {
					sendDecisionRequestToCurrentPeer();
				}
				else {
					// all peers are in recovery, so see if we can elect a leader.
					boolean ready = true;
					int max = upSetIntersection.stream().max(Integer::max).get();
					for (int i = 1; i <= max; ++i) {
						if (!recoveredProcesses.contains(i)) {
							ready = false;
							break;
						}
					}
					if (ready) {
						updateNodeUpSet();
						ownerNode.electNewLeader(uncommitted, false);					        
					}
					else {
						// the last process to fail hasn't recovered yet, so rewind
						advanceToNextProcessOrRewind();
					}
				}
			}
			else if (state == ParticipantRecoveryState.SomeProcessesUncertain) {
				// recovering process doesn't matter; keep walking the list
				advanceToNextProcessOrRewind();
			}
			break;
		case DECISION_REQUEST:
            ownerNode.send(overConnection, new InRecoveryResponse(uncommitted.getTransactionID(), originalUpSet));
			break;
		case STATE_REQUEST:
			ownerNode.send(overConnection, new UncertainResponse(uncommitted.getTransactionID()));
			ownerNode.becomeParticipantInRecovery(uncommitted, false);
			ownerNode.resetTimersFor(overConnection.getReceiverID());
			break;
		case UR_ELECTED:
			updateNodeUpSet();
			ownerNode.becomeCoordinatorInRecovery(uncommitted);			
			break;
        /* Fail Cases */
        case PARTIAL_BROADCAST:
            ownerNode.addFailure(message);
            break;
        case DEATH_AFTER:
            ownerNode.addFailure(message);
            break;

        case DELAY:
            Common.MESSAGE_DELAY = ((DelayMessage) message).getDelaySec()*1000;

		default:
			return false;
		}
		return true;
	}

	private void updateNodeUpSet() {
		Collection<PeerReference> nodeUpSet = uncommitted.getPeerSet().stream()
				.filter(pr -> recoveredProcesses.contains(pr.getNodeID()))
				.collect(Collectors.toList());
		ownerNode.setUpSet(nodeUpSet);
	}
	
	private void resetToNoInformation() {
		state = ParticipantRecoveryState.NoInformation;
		currentPeerIndex = 0;
		recoveredProcesses = new HashSet<Integer>();
		recoveredProcesses.add(ownerNode.getMyNodeID());
		upSetIntersection = new HashSet<Integer>();
		for (Integer peerId : originalUpSet) {
			upSetIntersection.add(peerId);
		}
	}

	private void sendDecisionRequestToCurrentPeer() {
		PeerReference current = sortedPeers.get(currentPeerIndex);
        Connection currentPeerConnection = ownerNode.isConnectedTo(current)
                ? ownerNode.getPeerConnForId(current.nodeID)
                : ownerNode.connectTo(current);
        ownerNode.send(currentPeerConnection, new DecisionRequest(uncommitted.getTransactionID()));
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
