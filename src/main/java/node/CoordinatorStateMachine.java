package node;

import messages.AbortRequest;
import messages.CommitRequest;
import messages.DelayMessage;
import messages.Message;
import messages.PeerTimeout;
import messages.PrecommitRequest;
import messages.StateRequest;
import messages.UncertainResponse;
import messages.YesResponse;
import messages.vote_req.VoteRequest;
import node.base.Node;
import node.base.StateMachine;
import system.failures.PartialBroadcast;
import system.network.Connection;
import util.Common;

import java.io.IOException;
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
		WaitingForStates
	}

	private CoordinatorState state;
	private Collection<Connection> txnConnections;
	private VoteRequest action;
	private int yesVotes;
	private int ongoingTransactionID;
	private int acks;

	// for termination protocol
	private int uncertainStates;
	private int precommits;

	public static CoordinatorStateMachine startInTerminationProtocol(Node ownerNode, VoteRequest action, boolean precommitted) {
		CoordinatorStateMachine machine = new CoordinatorStateMachine(ownerNode);
		machine.action = action;
		machine.ongoingTransactionID = action.getTransactionID();
		machine.state = CoordinatorState.WaitingForStates;
        machine.setPeerSet(action.getPeerSet());
        machine.acks = 1;
        if (precommitted) {
            machine.precommits = 1;
        }
        else {
            machine.uncertainStates = 1;
        }
		Collection<PeerReference> notMe = ownerNode.getUpSet().stream().filter(pr -> pr.getNodeID() != ownerNode.getMyNodeID()).collect(Collectors.toList());
        if (notMe.isEmpty()) {
            machine.txnConnections = new ArrayList<>();
            if (precommitted) {
                machine.commitCurrentAction();
            }
            else {
                machine.abortCurrentTransaction();
            }
        }
        else {
            machine.setupTransactionConnectionsAndSendMessage(new StateRequest(machine.ongoingTransactionID), notMe);
        }
		return machine;
	}

	public static CoordinatorStateMachine startInNormalMode(Node ownerNode) {
		return new CoordinatorStateMachine(ownerNode);
	}

	private CoordinatorStateMachine(Node ownerNode) {
        super(ownerNode);
		resetToWaiting();
	}

	public CoordinatorState getState() {
		return state;
	}

	public int getOngoingTransactionId() {
		return ongoingTransactionID;
	}

	public VoteRequest getAction() {
		return action;
	}

    @Override public boolean receiveMessage(Connection overConnection, Message message) {
        synchronized (this) {
            System.out.println("Coordinator "+ownerNode.getMyNodeID()+" "+
                               "received a "+message.getCommand()+" from "+overConnection.getReceiverID());

            switch (message.getCommand()) {
                case ADD:
                case UPDATE:
                case DELETE:
                    if (overConnection.getReceiverID() > 0
                     && overConnection.getReceiverID() < ownerNode.getMyNodeID())
                    {
                        ownerNode.becomeParticipant();
                        ownerNode.getStateMachine().receiveMessage(overConnection, message);
                    }
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
                case UNCERTAIN:
                	if (state == CoordinatorState.WaitingForStates) {
                		++uncertainStates;
                		checkForEnoughUncertainStates();
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
                    if (ownerNode.getUpSet() == null) {
                    } else {
                        PeerReference ref = getPeerSet().stream()
                                                        .filter(pr -> pr.nodeID == peerID)
                                                        .findFirst()
                                                        .get();
                        ownerNode.logMessage(message);
                        onTimeout(ref);
                    }
                    break;

                case COMMIT:
                	if (state == CoordinatorState.WaitingForStates) {
                		// remove the connection that sent us COMMIT from txnConnections so that we don't send it a commit.
                		txnConnections.removeIf(conn -> conn.getReceiverID() == overConnection.getReceiverID());
                		commitCurrentAction();
                	}
                	else if (state != CoordinatorState.WaitingForCommand) {
                		throw new RuntimeException("Received COMMIT when not expecting one");
                	}
                	break;
                case ABORT:
                	if (state == CoordinatorState.WaitingForStates) {
                		// remove the connection that sent us ABORT from txnConnections so that we don't send it an abort.
                		txnConnections.removeIf(conn -> conn.getReceiverID() == overConnection.getReceiverID());
                		abortCurrentTransaction();
                	}
                	else if (state != CoordinatorState.WaitingForCommand) {
                		throw new RuntimeException("Received COMMIT when not expecting one");
                	}
                	break;
                case PRE_COMMIT:
                	if (state == CoordinatorState.WaitingForStates) {
                		++precommits;
                		checkForEnoughUncertainStates();
                	}
                	else if (state != CoordinatorState.WaitingForCommand) {
                		throw new RuntimeException("Received PRE_COMMIT when not expecting one");
                	}
                	break;
                case DECISION_REQUEST:
                    /* reply with most-recent decision */
                    try {
                        if (state == CoordinatorState.WaitingForCommand) {
                            Message dec = ownerNode.getDecisionFor(message.getTransactionID());
                            if (dec == null) {
                                throw new RuntimeException("Couldn't find decision for txn "+message
                                        .getTransactionID());
                            }

                            ownerNode.send(overConnection, dec);
                        }


                    /* either
                        1. this coordinator is leading the charge for a recovery from total failure
                        2. we haven't come to a decision yet (unlikely)
                     */
                        else {
                            ownerNode.send(overConnection, new UncertainResponse(message.getTransactionID()));
                        }
                    }
                    catch (IOException e) {
                        ownerNode.getPeerConns().remove(overConnection);
                    }
                    break;

                /* fail cases */
                case PARTIAL_BROADCAST:
                case DEATH_AFTER:
                    ownerNode.addFailure(message);
                    break;

                case DELAY:
                    Common.MESSAGE_DELAY = ((DelayMessage) message).getDelaySec()*1000;
                    break;

                case IN_RECOVERY:
                case DUB_COORDINATOR:
                    /* ignore */
                    break;

                case UR_ELECTED:
                    try {
                        ownerNode.send(overConnection, ownerNode.getDecisionFor(message.getTransactionID()));
                    }
                    catch (IOException e) {
                        ownerNode.getPeerConns().remove(overConnection);
                    }
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
        final Connection refConn = ownerNode.getPeerConnForId(peerReference.nodeID);
        if (state == CoordinatorState.WaitingForVotes) {
    		getPeerSet().remove(peerReference);
            if (refConn != null) {
                txnConnections.remove(refConn);
            }
            abortCurrentTransaction();
    	}
    	else if (state == CoordinatorState.WaitingForAcks) {
    		getPeerSet().remove(peerReference);
            if (refConn != null) {
                txnConnections.remove(refConn);
            }
    		checkForEnoughAcks();
    	}
    }

    private void resetToWaiting() {
		ongoingTransactionID = NO_ONGOING_TRANSACTION;
		setPeerSet(null);
		txnConnections = null;
		state = CoordinatorState.WaitingForCommand;
		ownerNode.cancelAllTimers();
    }

    private void receivePlaylistCommand(VoteRequest message) {

    	if (ownerNode.getVoteValue(message))
    	{
	        // send vote requests to all peers
	        final Collection<PeerReference> peerSet =
	                message.getPeerSet().stream()
	                       .filter(ref -> ref.getNodeID() != ownerNode.getMyNodeID())
	                       .collect(Collectors.toList());

	    	ownerNode.logMessage(message);
            ownerNode.logMessage(new YesResponse(message));
	        setupTransactionConnectionsAndSendMessage(message, peerSet);
	        setPeerSet(peerSet);
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

	private void setupTransactionConnectionsAndSendMessage(Message message,
                                                           final Collection<PeerReference> peerSet)
    {
        Collection<Connection> conns = new ArrayList<>();

		/* connect to every peer the ownerNode is not already connected to */
		for (PeerReference reference : peerSet) {
            /* start timers on everyone */
            ownerNode.resetTimersFor(reference.getNodeID());
            Connection conn = null;
            try {
                conn = ownerNode.getOrConnectToPeer(reference);
                if (conn != null) {
                    conns.add(conn);
                    ownerNode.send(conn, message);
                } else {
                    ownerNode.log("Couldn't connect to "+reference.getNodeID());
                }
            }
            catch (IOException e) {
                System.err.println(ownerNode.getMyNodeID()+" couldn't send message to "+reference.getNodeID());
            }

		}
		txnConnections = conns;
	}

    private void abortCurrentTransaction() {
		AbortRequest abort = new AbortRequest(ongoingTransactionID);
		ownerNode.logMessage(abort);
        broadcast(abort);
		resetToWaiting();
    }

    private void checkForEnoughUncertainStates() {
    	final Collection<PeerReference> upSet = ownerNode.getUpSet();
    	if ((uncertainStates + precommits) >= upSet.size()) {
    		if (precommits == 0) {
        		abortCurrentTransaction();
    		}
    		else {
    			precommitCurrentAction();

                // shallow copy upset into peer set
                setPeerSet(upSet.stream().map(d->d).collect(Collectors.toList()));
    		}
    	}
    }

    private void checkForEnoughYesVotes() {
    	final Collection<PeerReference> peerSet = getPeerSet();
    	if (yesVotes >= peerSet.size()) {
    		precommitCurrentAction();
    	}
    }

    private void checkForEnoughAcks() {
    	final Collection<PeerReference> peerSet = getPeerSet();
		if (acks >= peerSet.size()) {
			commitCurrentAction();
		}
    }

    private int partialBroadcastCount(Message.Command stage) {
        final PartialBroadcast ptlBrdcst = ownerNode.getPartialBroadcast();
        int limit = 1000;
        if (ptlBrdcst != null && ptlBrdcst.getStage() == stage) {
            limit = ptlBrdcst.getCountProcs();
        }
        return limit;
    }

    private void precommitCurrentAction() {
        PrecommitRequest precommit = new PrecommitRequest(ongoingTransactionID);
        broadcast(precommit);
        txnConnections.forEach(conn -> ownerNode.resetTimersFor(conn.getReceiverID()));
		state = CoordinatorState.WaitingForAcks;
    }

    private void broadcast(Message message) {
        int limit = partialBroadcastCount(message.getCommand());
        int i = 0;
        for (Connection connection : txnConnections) {
            if (i++ >= limit) {
                ownerNode.selfDestruct();
            }
            try {
                ownerNode.send(connection, message);
            }
            catch (IOException e) {
                ownerNode.cancelTimersFor(connection.getReceiverID());
                onTimeout(getPeerSet().stream()
                                      .filter(p -> p.getNodeID() == connection.getReceiverID())
                                      .findFirst()
                                      .get());
                ownerNode.getPeerConns().remove(connection);
            }
        }
        if (message.getCommand().isDecision()) {
            ownerNode.sendTxnMgrMsg(message);
        }
        if (i == limit) {
            ownerNode.selfDestruct();
        }
    }

    private void commitCurrentAction() {
		CommitRequest commit = new CommitRequest(ongoingTransactionID);
		ownerNode.logMessage(commit);
        broadcast(commit);
        ownerNode.sendTxnMgrMsg(commit);
        ownerNode.applyActionToVolatileStorage(action);
        resetToWaiting();
    }
}
