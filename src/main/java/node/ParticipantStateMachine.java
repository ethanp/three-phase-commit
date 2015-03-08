package node;

import messages.AbortRequest;
import messages.AckRequest;
import messages.CommitRequest;
import messages.DelayMessage;
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
import node.system.SyncNode;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import system.network.Connection;
import util.Common;

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
        try { Thread.sleep(Common.MESSAGE_DELAY); }
        catch (InterruptedException ignored) {}

        synchronized (this) {

            System.out.println("Participant "+ownerNode.getMyNodeID()+" received a "+msg.getCommand()+" from "+currentConnection.getReceiverID());

            /* if it's from the Coordinator, reset their timeout timer */
            switch (msg.getCommand()) {
                case ADD:
                case UPDATE:
                case DELETE:
                case PRE_COMMIT:
                    ownerNode.resetTimersFor(currentConnection.getReceiverID());
                    break;
                case ABORT:
                case COMMIT:
                    ownerNode.cancelTimersFor(currentConnection.getReceiverID());
                    break;
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
                	Message decision = node.getDecisionFor(msg.getTransactionID());
                	if (decision != null) {
                		overConnection.sendMessage(decision);
                	}
                	else if (msg.getTransactionID() == ongoingTransactionID && precommitted) {
                		overConnection.sendMessage(new PrecommitRequest(ongoingTransactionID));
                	}
                	else {
                		overConnection.sendMessage(new UncertainResponse(msg.getTransactionID()));
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
                	overConnection.sendMessage(m);
                	ownerNode.resetTimersFor(overConnection.getReceiverID());
                	break;

                /* SET FAIL-CASE OR DELAY */
                case PARTIAL_BROADCAST:
                case DEATH_AFTER:
                    ownerNode.addFailure(msg);
                    break;

                /* SET INTERACTIVE DELAY */
                case DELAY:
                    Common.MESSAGE_DELAY = ((DelayMessage) msg).getDelaySec()*1000;
                    break;

                default:
                    throw new RuntimeException("Not a valid message: "+msg.getCommand());
            }
        }

        return true;
    }

    Message lastLoggedMessage() {
    	List<Message> loggedMessages = ownerNode.getDtLog().getLoggedMessages().stream().collect(Collectors.toList());
		int s = loggedMessages.size();
		return s > 0 ? loggedMessages.get(s - 1) : null;
	}

    private void receiveUR_ELECTED(Message message) {
        ownerNode.getUpSet().removeIf(n -> n.getNodeID() < ownerNode.getMyNodeID());
        ownerNode = new SyncNode(3, null);
        ownerNode.becomeCoordinatorInRecovery(action, precommitted);
    }

    private void receiveAbort(Message message) {
        ownerNode.logMessage(message);
        action = null;
        setPeerSet(null);
        ownerNode.setUpSet(null);
    }

    private void receiveDubCoordinator(Message message) {
        ownerNode.becomeCoordinator();
    }

    private void receiveVoteRequest(VoteRequest voteRequest, boolean voteValue) {
    	setCoordinatorID(currentConnection.getReceiverID());
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
        ownerNode.send(currentConnection, new AckRequest(getOngoingTransactionID()));
    }

    private void receiveCommit(CommitRequest commitRequest) {
        ownerNode.logMessage(commitRequest);
        ownerNode.applyActionToVolatileStorage(action);
        setOngoingTransactionID(NO_ONGOING_TRANSACTION);
    }

    public void respondNOToVoteRequest(Message message) {
        setOngoingTransactionID(NO_ONGOING_TRANSACTION);
        ownerNode.logMessage(new AbortRequest(message.getTransactionID()));
        ownerNode.send(currentConnection, new NoResponse(message));
    }

    private void respondYESToVoteRequest(VoteRequest voteRequest) {
        action = voteRequest;
        setOngoingTransactionID(voteRequest.getTransactionID());
        setPeerSet(voteRequest.getCloneOfPeerSet());
        ownerNode.setUpSet(voteRequest.getCloneOfPeerSet());
        ownerNode.resetTimersFor(currentConnection.getReceiverID());
        logAndSendMessage(new YesResponse(voteRequest));
    }

    public void logAndSendMessage(Message message) {
        ownerNode.logMessage(message);
        ownerNode.send(currentConnection, message);
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
	    	ownerNode.logMessage(timeout);
	        removeFromUpset(timeout.getPeerId());
            ownerNode.electNewLeader(action, precommitted);
            ownerNode.sendTxnMgrMsg(timeout);
    	}
        else {
            throw new RuntimeException("timeout on ownerNode ["+timeout.getPeerId()+"] "+
                                       "not known to be coordinator");
        }
    }

    private PeerReference getNodeWithLowestIDInUpset() {
        if (ownerNode.getUpSet().isEmpty()) return null;
        PriorityQueue<PeerReference> peerReferences = new PriorityQueue<>(ownerNode.getUpSet());
        return peerReferences.poll();
    }

    private void removeFromUpset(int id) {
        ownerNode.setUpSet(ownerNode.getUpSet()
                          .stream()
                          .filter(c -> c.getNodeID() != id)
                          .collect(Collectors.toList()));
    }
}
