package node;

import messages.AbortRequest;
import messages.AckRequest;
import messages.CommitRequest;
import messages.Message;
import messages.NoResponse;
import messages.PeerTimeout;
import messages.PrecommitRequest;
import messages.UncertainResponse;
import messages.YesResponse;
import messages.vote_req.AddRequest;
import messages.vote_req.DeleteRequest;
import messages.vote_req.UpdateRequest;
import messages.vote_req.VoteRequest;
import node.base.Node;
import node.base.StateMachine;
import system.network.Connection;
import util.Common;

import java.io.IOException;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import static util.Common.NO_ONGOING_TRANSACTION;

/**
 * Ethan Petuchowski 2/27/15
 */
public class ParticipantStateMachine extends StateMachine {

    /* ONGOING TRANSACTION ATTRIBUTES */
    private int ongoingTransactionID = NO_ONGOING_TRANSACTION;
    private VoteRequest action;  // the update being performed
    private boolean precommitted = false;
    private Connection currentConnection = null;
    private Connection coordinatorConnection = null;
    private int coordinatorId;

    public static ParticipantStateMachine startInTerminationProtocol(Node ownerNode, VoteRequest action, boolean precommit) {
        ParticipantStateMachine machine = new ParticipantStateMachine(ownerNode);
    	machine.action = action;
    	machine.ongoingTransactionID = action.getTransactionID();
    	machine.precommitted = precommit;
    	return machine;
    }

    public ParticipantStateMachine(Node node) {
        super(node);
    }

    @Override public boolean receiveMessage(Connection overConnection, Message msg) {
        currentConnection = overConnection;
        synchronized (this) {

            System.out.println("Participant "+ownerNode.getMyNodeID()+" received a "+msg.getCommand()+" from "+currentConnection.getReceiverID());

            if (currentConnection.getReceiverID() == coordinatorId) {
                switch (msg.getCommand()) {
                    case ADD:
                    case UPDATE:
                    case DELETE:
                    case PRE_COMMIT:
                    case STATE_REQUEST:
                        ownerNode.resetTimersFor(currentConnection.getReceiverID());
                        break;
                    case ABORT:
                    case COMMIT:
                        ownerNode.cancelAllTimers();
                        break;
                }
            }

            switch (msg.getCommand()) {

                /* VOTE REQUESTS */
                case ADD:
                case UPDATE:
                case DELETE:
                    VoteRequest vote = (VoteRequest)msg;
                     receiveVoteRequest(vote, ownerNode.getVoteValue(vote));
                     break;

                /* PROTOCOL STAGES */
                case PRE_COMMIT:
                    receivePrecommit((PrecommitRequest) msg);
                    break;
                case COMMIT:
                    receiveCommit((CommitRequest) msg);
                    break;
                case ABORT:
                    receiveAbort(msg);
                    break;

                /* BECOME COORDINATOR */
                case UR_ELECTED:
                    receiveUR_ELECTED(msg);
                    break;
                case NONE:
                    break;
                case DUB_COORDINATOR:
                    receiveDubCoordinator(msg);
                    break;
                /* UH-OH */
                case TIMEOUT:
                    onTimeout((PeerTimeout) msg);
                    break;

                case DECISION_REQUEST:
                	Message decision = ownerNode.getDecisionFor(msg.getTransactionID());
                    try {
                        if (decision != null) {
                            overConnection.sendMessage(decision);
                        }
                        else if (msg.getTransactionID() == ongoingTransactionID && precommitted) {
                            overConnection.sendMessage(new PrecommitRequest(ongoingTransactionID));
                        }
                        else {
                            overConnection.sendMessage(new UncertainResponse(msg.getTransactionID()));
                        }
                    }
                    catch (IOException e) {
                        ownerNode.log("Unable to reply to "+overConnection.getReceiverID());
                    }
                	break;

                case STATE_REQUEST:
                    ownerNode.getUpSet().removeIf(n -> n.getNodeID() < overConnection.getReceiverID());
                    coordinatorId = overConnection.getReceiverID();
                    Message m, lastLogged = lastLoggedMessage();
                	if (lastLogged == null) {
                		m = new AbortRequest(Common.NO_ONGOING_TRANSACTION);
                	}
                	else if (lastLogged instanceof AbortRequest || lastLogged instanceof CommitRequest) {
                		m = lastLogged;
                	}
                	else {
                		m = precommitted ? new PrecommitRequest(ongoingTransactionID) : new UncertainResponse(action.getTransactionID());
                	}
                    try {
                        overConnection.sendMessage(m);
                    }
                    catch (IOException e) {
                        forceTimeoutBcBrokenConn(overConnection);
                    }
                    break;

                default:
                    ownerNode.log("Not a valid message: "+msg.getCommand());
            }
        }

        return true;
    }

    private void forceTimeoutBcBrokenConn(Connection conn) {
        ownerNode.cancelTimersFor(conn.getReceiverID());
        onTimeout(new PeerTimeout(conn.getReceiverID()));
        ownerNode.getPeerConns().remove(conn);
    }

    Message lastLoggedMessage() {
    	List<Message> loggedMessages = ownerNode.getDtLog().getLoggedMessages().stream().collect(Collectors.toList());
		int s = loggedMessages.size();
		return s > 0 ? loggedMessages.get(s - 1) : null;
	}

    private void receiveUR_ELECTED(Message message) {
        if (action == null) {
            /* decision was already reached */
            final Message decision = ownerNode.getDecisionFor(message.getTransactionID());
            try {
                currentConnection.sendMessage(decision);
            }
            catch (IOException e) {
                /* ignore */
            }
            ownerNode.sendTxnMgrMsg(decision);
        }
        else {
            ownerNode.getUpSet().removeIf(n -> n.getNodeID() < ownerNode.getMyNodeID());
            ownerNode.becomeCoordinatorInRecovery(action, precommitted);
        }
    }

    private void receiveAbort(Message message) {
        ownerNode.logMessage(message);
        action = null;
        setPeerSet(null);
        ownerNode.setUpSet(null);
        setOngoingTransactionID(NO_ONGOING_TRANSACTION);
    }

    private void receiveDubCoordinator(Message message) {
        ownerNode.becomeCoordinator();
    }

    private void receiveVoteRequest(VoteRequest voteRequest, boolean voteValue) {
    	setCoordinatorID(currentConnection.getReceiverID());
        coordinatorConnection = currentConnection;
        ownerNode.setUpSet(voteRequest.getPeerSet());
    	ownerNode.logMessage(voteRequest);
        if (voteValue) {
            respondYESToVoteRequest(voteRequest);
        }
        else {
            respondNOToVoteRequest(voteRequest);
        }
    }

    private void receiveAddRequest(AddRequest addRequest) {
        receiveVoteRequest(addRequest, !ownerNode.hasSongTupleWithName(addRequest.getSongTuple()));
    }

    private void receiveUpdateRequest(UpdateRequest updateRequest) {
        receiveVoteRequest(updateRequest, ownerNode.hasSong(updateRequest.getSongName()));
    }

    private void receiveDeleteRequest(DeleteRequest deleteRequest) {
        receiveVoteRequest(deleteRequest, ownerNode.hasSong(deleteRequest.getSongName()));
    }

    /**
     * doesn't log anything (Lecture 3, Pg. 13), send ACK
     */
    private void receivePrecommit(PrecommitRequest precommitRequest) {
        setPrecommitted(true);
        try {
            ownerNode.send(currentConnection, new AckRequest(getOngoingTransactionID()));
        }
        catch (IOException e) {
            forceTimeoutBcBrokenConn(currentConnection);
        }
    }

    private void receiveCommit(CommitRequest commitRequest) {
        ownerNode.logMessage(commitRequest);
        ownerNode.applyActionToVolatileStorage(action);
        setOngoingTransactionID(NO_ONGOING_TRANSACTION);
        action = null;
    }

    public void respondNOToVoteRequest(Message message) {
        receiveAbort(new AbortRequest(message.getTransactionID()));
        try {
            ownerNode.send(currentConnection, new NoResponse(message));
        }
        catch (IOException e) {
            forceTimeoutBcBrokenConn(currentConnection);
        }
    }

    private void respondYESToVoteRequest(VoteRequest voteRequest) {
        action = voteRequest;
        setOngoingTransactionID(voteRequest.getTransactionID());
        setPeerSet(voteRequest.getCloneOfPeerSet());
        ownerNode.setUpSet(voteRequest.getCloneOfPeerSet());
        ownerNode.resetTimersFor(currentConnection.getReceiverID());
        final YesResponse response = new YesResponse(voteRequest);
        ownerNode.logMessage(response);
        try {
            ownerNode.send(currentConnection, response);
        }
        catch (IOException e) {
            forceTimeoutBcBrokenConn(currentConnection);
        }
    }

    /* Getters and Setters */

    public int getOngoingTransactionID() {
        return ongoingTransactionID;
    }

    public void setOngoingTransactionID(int ongoingTransactionID) {
        this.ongoingTransactionID = ongoingTransactionID;
    }

    public boolean isPrecommitted() {
        return precommitted;
    }

    public void setPrecommitted(boolean precommitted) {
        this.precommitted = precommitted;
    }

    public void setCoordinatorID(int coordinatorID) {
        this.coordinatorId = coordinatorID;
    }

    public VoteRequest getAction() {
        return action;
    }

    public void setAction(VoteRequest action) {
        this.action = action;
    }

    private void onTimeout(PeerTimeout timeout) {
        if (timeout.getPeerId() == coordinatorId) {
            ownerNode.sendTxnMgrMsg(timeout);

            if (action != null) {
                ownerNode.logMessage(timeout);
                removeFromUpset(timeout.getPeerId());
                ownerNode.electNewLeader(action, precommitted);
            }
    	}
    }

    private PeerReference getNodeWithLowestIDInUpset() {
        if (ownerNode.getUpSet().isEmpty()) return null;
        PriorityQueue<PeerReference> peerReferences = new PriorityQueue<>(ownerNode.getUpSet());
        return peerReferences.poll();
    }

    private void removeFromUpset(int id) {
        if (ownerNode.getUpSet() == null) return;
        ownerNode.setUpSet(ownerNode.getUpSet()
                          .stream()
                          .filter(c -> c.getNodeID() != id)
                          .collect(Collectors.toList()));
    }

    public void setCoordinatorConnection(Connection coordinatorConnection) {
        this.coordinatorConnection = coordinatorConnection;
    }
}
