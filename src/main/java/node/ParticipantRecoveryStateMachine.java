package node;

import messages.DecisionRequest;
import messages.DelayMessage;
import messages.InRecoveryResponse;
import messages.Message;
import messages.Message.Command;
import messages.PeerTimeout;
import messages.UncertainResponse;
import messages.vote_req.VoteRequest;
import node.base.Node;
import node.base.StateMachine;
import system.network.Connection;
import util.Common;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ParticipantRecoveryStateMachine extends StateMachine {
	private enum ParticipantRecoveryState {
		NoInformation,
		SomeProcessesUncertain,
		SomeProcessesInRecovery
	};

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
		super(ownerNode);
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

        StringBuilder sb1 = new StringBuilder();
        sortedPeers.forEach(p -> sb1.append(p.getNodeID()+" "));
        StringBuilder sb2 = new StringBuilder();
        originalUpSet.forEach(p -> sb2.append(p+" "));
        ownerNode.log("Starting with peers "+sb1.toString()+" and upset "+sb2.toString());

		resetToNoInformation();
	}

	@Override
    public synchronized boolean receiveMessage(Connection overConnection, Message message) {
        int receiverID = overConnection.getReceiverID();
        ownerNode.cancelTimersFor(overConnection.getReceiverID());

        if (!message.getCommand().equals(Command.TIMEOUT) && receiverID > 0) {
            recoveredProcesses.add(receiverID);
        }
        else {
            recoveredProcesses.remove(receiverID);
        }

        System.out.println("ParticipantInRec "+ownerNode.getMyNodeID()+"" +" received a "+
                           message.getCommand()+" from "+overConnection.getReceiverID());
        switch (message.getCommand()) {

            case COMMIT:
                if (state == ParticipantRecoveryState.NoInformation ||
                    state == ParticipantRecoveryState.SomeProcessesInRecovery) {
                    ownerNode.logMessage(message);
                    ownerNode.applyActionToVolatileStorage(uncommitted);
                    ownerNode.becomeParticipant();
                }
                else /* some processes uncertain! */
                    throw new RuntimeException("Received commit response in a state where we weren't expecting one.");
                break;

            case ABORT:
                ownerNode.logMessage(message);
                ownerNode.becomeParticipant();
                break;

            case PRE_COMMIT:
                advanceToNextProcessOrRewind();
                break;

            case UNCERTAIN:
                state = ParticipantRecoveryState.SomeProcessesUncertain;
                advanceToNextProcessOrRewind();
                break;

            case TIMEOUT:
                advanceToNextProcessOrRewind();
                break;

            case IN_RECOVERY:
                if (state == ParticipantRecoveryState.NoInformation ||
                    state == ParticipantRecoveryState.SomeProcessesInRecovery) {

                    state = ParticipantRecoveryState.SomeProcessesInRecovery;
                    InRecoveryResponse inRecovery = (InRecoveryResponse) message;
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
                try {
                    ownerNode.send(overConnection, new InRecoveryResponse(uncommitted.getTransactionID(), originalUpSet));
                }
                catch (IOException e) {
                    ownerNode.getPeerConns().remove(overConnection);
                }
                break;
            case STATE_REQUEST:
                try {
                    ownerNode.send(overConnection, new UncertainResponse(uncommitted.getTransactionID()));
                    ownerNode.becomeParticipantInTerminationProtocol(uncommitted, false);
                    ownerNode.resetTimersFor(overConnection.getReceiverID());
                }
                catch (IOException e) {
                    ownerNode.getPeerConns().remove(overConnection);
                }

                break;
            case UR_ELECTED:
                updateNodeUpSet();
                ownerNode.becomeCoordinatorInRecovery(uncommitted, false);
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

    public void sendDecisionRequestToCurrentPeer() {
        PeerReference current = sortedPeers.get(currentPeerIndex);
        final int currentNodeID = current.getNodeID();
        System.out.println("Node "+ownerNode.getMyNodeID()+": sending DEC_REC to "+currentNodeID);
        try {
//            System.out.println("Node "+ownerNode.getMyNodeID()+" adding timer for "+current.getNodeID());
            Connection currentPeerConnection = ownerNode.getOrConnectToPeer(current);
            ownerNode.log("obtained conn to "+currentNodeID);
            ownerNode.send(currentPeerConnection, new DecisionRequest(uncommitted.getTransactionID()));
            ownerNode.resetTimersFor(currentNodeID);
        }
        catch (IOException e) {
            System.err.println("Node "+ownerNode.getMyNodeID()+": " +
                               "could not send DEC_REC to "+currentNodeID);
            ownerNode.sendTxnMgrMsg(new PeerTimeout(currentNodeID));
            advanceToNextProcessOrRewind();
        }
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
                System.err.println("Node "+ownerNode.getMyNodeID()+"'s wheel-clock was interrupted by node timer");
                try {
                    Thread.sleep(300);
                }
                catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
			resetToNoInformation();
			sendDecisionRequestToCurrentPeer();
		}
	}
}
