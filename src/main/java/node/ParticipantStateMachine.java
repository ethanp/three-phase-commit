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
import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import system.network.Connection;
import util.Common;

import java.io.EOFException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static util.Common.NO_ONGOING_TRANSACTION;

/**
 * Ethan Petuchowski 2/27/15
 */
public class ParticipantStateMachine extends StateMachine {

	
    /* REFERENCES */
    final Node node;

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
        this.node = node;
    }

    @Override public boolean receiveMessage(Connection overConnection) {
        currentConnection = overConnection;

        Message msg;

        try { msg = currentConnection.receiveMessage(); }
        catch (EOFException e) {
            System.err.println("Node "+node.getMyNodeID()+" received EOFException from "+overConnection.getReceiverID());
            System.exit(Common.EXIT_FAILURE);
            msg = null;
        }

        if (msg == null) {
            return false;
        }

        try { Thread.sleep(Common.MESSAGE_DELAY); }
        catch (InterruptedException ignored) {}

        synchronized (this) {

            System.out.println("Participant "+node.getMyNodeID()+" received a "+msg.getCommand()+" from "+currentConnection.getReceiverID());

            /* if it's from the Coordinator, reset their timeout timer */
            switch (msg.getCommand()) {
                case ADD:
                case UPDATE:
                case DELETE:
                case PRE_COMMIT:
                    node.resetTimersFor(currentConnection.getReceiverID());
                case ABORT:
                case COMMIT:
                    node.cancelTimersFor(currentConnection.getReceiverID());
            }

            switch (msg.getCommand()) {

                /* VOTE REQUESTS */
                case ADD:
                case UPDATE:
                case DELETE:
                    VoteRequest vote = (VoteRequest)msg;
                     receiveVoteRequest(vote, node.getVoteValue(vote));
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

                // TODO Decision-Request
                case DECISION_REQUEST:
                    throw new NotImplementedException();
                case STATE_REQUEST:
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
                	node.resetTimersFor(overConnection.getReceiverID());
                	break;
                	
                /* SET FAIL-CASE OR DELAY */
                case PARTIAL_BROADCAST:
                case DEATH_AFTER:
                    node.addFailure(msg);
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
    	List<Message> loggedMessages = node.getDtLog().getLoggedMessages().stream().collect(Collectors.toList());
		int s = loggedMessages.size();
		return s > 0 ? loggedMessages.get(s - 1) : null;
	}    

    private void receiveUR_ELECTED(Message message) {

        // TODO we can remove members of UP-set with peerID < myPeerID

        node.becomeCoordinatorInRecovery(action);
    }

    private void receiveAbort(Message message) {
        node.logMessage(message);
        action = null;
        setPeerSet(null);
        node.setUpSet(null);
    }

    private void receiveDubCoordinator(Message message) {
        node.becomeCoordinator();
    }

    private void receiveVoteRequest(VoteRequest voteRequest, boolean voteValue) {
    	setCoordinatorID(currentConnection.getReceiverID());
    	node.logMessage(voteRequest);
        if (voteValue) {
            respondYESToVoteRequest(voteRequest);
        }
        else {
            respondNOToVoteRequest(voteRequest);
        }
    }

    private void receiveAddRequest(AddRequest addRequest) {
        receiveVoteRequest(addRequest, !node.hasSongTupleWithName(addRequest.getSongTuple()));
    }

    private void receiveUpdateRequest(UpdateRequest updateRequest) {
        receiveVoteRequest(updateRequest, node.hasSong(updateRequest.getSongName()));
    }

    private void receiveDeleteRequest(DeleteRequest deleteRequest) {
        receiveVoteRequest(deleteRequest, node.hasSong(deleteRequest.getSongName()));
    }

    /**
     * doesn't log anything (Lecture 3, Pg. 13), send ACK
     */
    private void receivePrecommit(PrecommitRequest precommitRequest) {
        setPrecommitted(true);
        node.send(currentConnection, new AckRequest(getOngoingTransactionID()));
    }

    private void receiveCommit(CommitRequest commitRequest) {
        node.logMessage(commitRequest);
        node.commitAction(action);
        setOngoingTransactionID(NO_ONGOING_TRANSACTION);
    }

    public void respondNOToVoteRequest(Message message) {
        setOngoingTransactionID(NO_ONGOING_TRANSACTION);
        node.logMessage(new AbortRequest(message.getTransactionID()));
        node.send(currentConnection, new NoResponse(message));
    }

    private void respondYESToVoteRequest(VoteRequest voteRequest) {
        action = voteRequest;
        setOngoingTransactionID(voteRequest.getTransactionID());
        setPeerSet(voteRequest.getCloneOfPeerSet());
        node.setUpSet(voteRequest.getCloneOfPeerSet());
        logAndSendMessage(new YesResponse(voteRequest));
    }

    public void logAndSendMessage(Message message) {
        node.logMessage(message);
        node.send(currentConnection, message);
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
	    	node.logMessage(timeout);
	        removeFromUpset(timeout.getPeerId());
            node.electNewLeader(action, precommitted);
    	}
        else {
            throw new RuntimeException("timeout on node ["+timeout.getPeerId()+"] "+
                                       "not known to be coordinator");
        }
    }

    private PeerReference getNodeWithLowestIDInUpset() {
        if (node.getUpSet().isEmpty()) return null;
        PriorityQueue<PeerReference> peerReferences = new PriorityQueue<>(node.getUpSet());
        return peerReferences.poll();
    }

    private void removeFromUpset(int id) {
        node.setUpSet(node.getUpSet().stream().filter(c -> c.getNodeID() != id).collect(Collectors.toList()));
    }
}
