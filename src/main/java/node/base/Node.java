package node.base;

import messages.ElectedMessage;
import messages.Message;
import messages.PeerTimeout;
import messages.TokenWriter;
import messages.vote_req.AddRequest;
import messages.vote_req.DeleteRequest;
import messages.vote_req.UpdateRequest;
import messages.vote_req.VoteRequest;
import node.CoordinatorStateMachine;
import node.LogRecoveryStateMachine;
import node.ParticipantRecoveryStateMachine;
import node.ParticipantStateMachine;
import node.PeerReference;
import system.failures.DeathAfter;
import system.failures.PartialBroadcast;
import system.network.Connection;
import system.network.MessageReceiver;
import system.network.QueueConnection;
import util.Common;
import util.SongTuple;

import java.io.EOFException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static util.Common.TIMEOUT_MONITOR_ID;

/**
 * Ethan Petuchowski 2/27/15
 */
public abstract class Node implements MessageReceiver {

    protected StateMachine stateMachine;
    protected final int myNodeID;
    protected final Set<SongTuple> playlist = new TreeSet<>();
    protected DTLog dtLog;
    protected final TimeoutMonitor timeoutMonitor;

    protected Connection txnMgrConn;
    protected Collection<Connection> peerConns = new ArrayList<>();
    private Collection<PeerReference> upSet = null;

    protected PartialBroadcast partialBroadcast = null;
    protected DeathAfter deathAfter = null;

    protected int msgsSent = 0;

    public Node(int myNodeID) {
        this.myNodeID = myNodeID;
        stateMachine = new ParticipantStateMachine(this);
        this.timeoutMonitor = new TimeoutMonitor();
    }

    public void addConnection(Connection connection) {
        peerConns.add(connection);
    }

    public int getMyNodeID() {
        return myNodeID;
    }

    public DTLog getDtLog() {
        return dtLog;
    }

    public void setDtLog(DTLog dtLog) {
        this.dtLog = dtLog;
    }

    public void log(String s) {
        System.out.println(getMyNodeID()+": "+s);
    }

    public void recoverFromDtLog() {
    	LogRecoveryStateMachine recoveryMachine = new LogRecoveryStateMachine(this);
    	VoteRequest uncommitted = recoveryMachine.getUncommittedRequest();
    	if (uncommitted == null || !recoveryMachine.didVoteYesOnRequest()) {
    		stateMachine = new ParticipantStateMachine(this);
    	}
    	else {
    		stateMachine = new ParticipantRecoveryStateMachine(this, uncommitted, recoveryMachine.getLastUpSet());
            ((ParticipantRecoveryStateMachine)stateMachine).sendDecisionRequestToCurrentPeer();
    	}
    }

    public void logMessage(Message message) {
    	TokenWriter writer = dtLog.beginLog();
        Message.writeMessage(message, writer);
        dtLog.endLog(writer);
    }

    public boolean getVoteValue(VoteRequest vote) {
    	switch (vote.getCommand()) {
    	case ADD:
    		return !hasSongTupleWithName(((AddRequest)vote).getSongTuple());
    	case UPDATE:
    		return hasSong(((UpdateRequest)vote).getSongName());
    	case DELETE:
    		return hasSong(((DeleteRequest)vote).getSongName());
		default:
    		return false;
    	}
    }

    public boolean hasSongTupleWithName(SongTuple tuple) {
        return playlist.contains(tuple);
    }

    public boolean hasExactSongTuple(SongTuple tuple) {
        for (SongTuple songTuple : playlist) {
            if (songTuple.toLogString().equals(tuple.toLogString())) {
                return true;
            }
        }
        return false;
    }

    public boolean hasNoSongs() {
    	return playlist.size() == 0;
    }

    public boolean hasSong(String name) {
        return playlist.contains(new SongTuple(name, "doesn't matter"));
    }

    public void applyActionToVolatileStorage(VoteRequest action) {
        switch (action.getCommand()) {
        case ADD:
            applyAddToVolatilePlaylist((AddRequest) action);
            break;
        case UPDATE:
            applyUpdateToVolatilePlaylist((UpdateRequest) action);
            break;
        case DELETE:
            applyDeleteToVolatilePlaylist((DeleteRequest) action);
            break;
        default:
        	break;
	    }
    }

    private void applyDeleteToVolatilePlaylist(DeleteRequest deleteRequest) {
        removeSongWithName(deleteRequest.getSongName());
    }

    private void applyUpdateToVolatilePlaylist(UpdateRequest updateRequest) {
        updateSong(updateRequest.getSongName(), updateRequest.getUpdatedSong());
    }

    private void applyAddToVolatilePlaylist(AddRequest addRequest) {
        addSongToPlaylist(addRequest.getSongTuple());
    }

    public boolean addSongToPlaylist(SongTuple songTuple) {
        return playlist.add(songTuple);
    }

    public boolean removeSongWithName(String songName) {
        return playlist.remove(new SongTuple(songName, ""));
    }

    public void updateSong(String songName, SongTuple updatedSong) {
        removeSongWithName(songName);
        addSongToPlaylist(updatedSong);
    }

    @Override public boolean receiveMessageFrom(Connection connection, int msgsRcvd) {
        final int otherEnd = connection.getReceiverID();
        if (deathAfter != null
            && deathAfter.getFromProc() == otherEnd
            && msgsRcvd >= deathAfter.getNumMsgs())
        {
            System.out.println(getMyNodeID()+" received too many messages from "+otherEnd);
            selfDestruct();
        }

        Message message = null;
        try {
            message = connection.receiveMessage();
        }
        catch (EOFException e) {
            System.err.println("Ignored EOFE");
        }
        if (message != null) {
            synchronized (this) {
                return stateMachine.receiveMessage(connection, message);
            }
        }
        return false;
    }

    public void becomeCoordinator() {
        stateMachine = CoordinatorStateMachine.startInNormalMode(this);
    }

    public void becomeCoordinatorInRecovery(VoteRequest ongoingAction, boolean precommitted) {
    	stateMachine = CoordinatorStateMachine.startInTerminationProtocol(this, ongoingAction, precommitted);
    }

    public void becomeParticipant() {
    	stateMachine = new ParticipantStateMachine(this);
    }

    public void becomeParticipantInRecovery(VoteRequest ongoingAction, boolean precommit) {
    	stateMachine = ParticipantStateMachine.startInTerminationProtocol(this, ongoingAction, precommit);
    }

    public StateMachine getStateMachine() {
        return stateMachine;
    }

    public Collection<Connection> getPeerConns() {
        return peerConns;
    }

    public Connection getPeerConnForId(int id) {
    	return peerConns.stream().filter(c -> c.getReceiverID() == id).findFirst().get();
    }

    public void sendTxnMgrMsg(Message message) {
        send(txnMgrConn, message);
    }

    public void send(Connection conn, Message message) {
        msgsSent++;
        conn.sendMessage(message);
    }

    /**
     * calling this method should make this peer acquire a connection to the referenced peer
     * AND should ('eventually') make that peer acquire a reciprocal connection back to this peer
     *
     * In the synchronous case, this means directly adding each end of the QueueSocket to each
     * peer's `peerConns` collection
     *
     * In the asynchronous case, it means establishing a socket with the referenced peer's server
     */
    public abstract Connection connectTo(PeerReference peerReference);

    public boolean isConnectedTo(PeerReference reference) {
        return peerConns.stream()
                        .filter(conn -> conn.getReceiverID() == reference.getNodeID())
                        .count() > 0;
    }

    public void setPeerConns(Collection<Connection> peerConns) {
        this.peerConns = peerConns;
    }

    public Collection<PeerReference> getUpSet() {
        return upSet;
    }

    public void setUpSet(Collection<PeerReference> upSet) {
        this.upSet = upSet;
    }

    public void resetTimersFor(int peerID) {
        timeoutMonitor.resetTimersFor(peerID);
    }

    public void cancelTimersFor(int peerID) {
        timeoutMonitor.cancelTimersFor(peerID);
    }

    public void electNewLeader(VoteRequest ongoingAction, boolean precommitted) {
    	PeerReference newCoordinator = upSet.stream().min((a, b) -> a.compareTo(b)).get();
    	if (newCoordinator.getNodeID() == myNodeID) {
    		becomeCoordinatorInRecovery(ongoingAction, precommitted);
    	}
    	else {
    		getOrConnectToPeer(newCoordinator).sendMessage(new ElectedMessage(ongoingAction.getTransactionID()));
    		resetTimersFor(newCoordinator.getNodeID());
    		stateMachine = ParticipantStateMachine.startInTerminationProtocol(this, ongoingAction, precommitted);
            ((ParticipantStateMachine)stateMachine).setCoordinatorID(newCoordinator.getNodeID());
    	}
    }

    public Connection getOrConnectToPeer(PeerReference peer) {
        Connection connection = isConnectedTo(peer)
                ? getPeerConnForId(peer.getNodeID())
                : connectTo(peer);
        return connection;
    }

    public void addFailure(Message msg) {
        if (msg instanceof PartialBroadcast) {
            partialBroadcast = (PartialBroadcast) msg;
        }
        else if (msg instanceof DeathAfter) {
            deathAfter = (DeathAfter) msg;
            System.out.println(getMyNodeID()+" will die after "+deathAfter.getNumMsgs()+" msgs from "+deathAfter.getFromProc());
        }
    }

    public void broadcast(Collection<Connection> recipients, Message request) {
        int i = 0, numBeforeCrash = 1000;
        if (partialBroadcast != null && partialBroadcast.getStage().equals(request.getCommand())) {
            numBeforeCrash = partialBroadcast.getCountProcs();
        }
        for (Connection connection : recipients) {
            if (i++ < numBeforeCrash) {
                send(connection, request);
            }
            else {
                selfDestruct();
            }
		}
        sendTxnMgrMsg(request);
    }

    public abstract void selfDestruct();

    public Message getDecisionFor(int transactionID) {
        List<Message> messages = new ArrayList<>(getDtLog().getLoggedMessages());
        Collections.reverse(messages);
        return messages.stream()
                       .filter(m -> m.getTransactionID() == transactionID
                                    && m.getCommand().isDecision())
                       .findFirst()
                       .orElse(null);
    }

    public PartialBroadcast getPartialBroadcast() {
        return partialBroadcast;
    }

    private class TimeoutMonitor {

        /* timers by peerID */
        Map<Integer, List<Thread>> ongoingTimers = new HashMap<>();

        void startTimer(int peerID) {
            if (ongoingTimers.containsKey(peerID)) {
                ongoingTimers.get(peerID).add(createTimer(peerID));
            }
            else {
                ongoingTimers.put(peerID, new ArrayList<>(Arrays.asList(createTimer(peerID))));
            }
        }

        void cancelTimersFor(int peerID) {
            if (ongoingTimers.containsKey(peerID)) {
                ongoingTimers.get(peerID).stream().forEach(Thread::interrupt);
                ongoingTimers.remove(peerID);
            }
        }

        void resetTimersFor(int peerID) {
            cancelTimersFor(peerID);
            startTimer(peerID);
        }

        private Thread createTimer(int peerID) {
            final Thread thread = new Thread(new TimeoutTimer(peerID, Common.TIMEOUT_MILLISECONDS));
            thread.start();
            return thread;
        }

        class TimeoutTimer implements Runnable {

            final int peerID;
            final int milliseconds;

            TimeoutTimer(int peerID, int milliseconds) {
                this.peerID = peerID;
                this.milliseconds = milliseconds;
            }

            @Override public void run() {
                try {
                    Thread.sleep(milliseconds);

                    if (Thread.interrupted()) return;

                    /* this means a Timeout DID occur */
                    cancelTimersFor(peerID);

                    receiveMessageFrom(
                            new QueueConnection(
                                    TIMEOUT_MONITOR_ID,
                                    new LinkedList<>(Arrays.asList(new PeerTimeout(peerID))),
                                    new LinkedList<>()),
                            0);
                }
                catch (InterruptedException e) {
                    /* this means a Timeout did NOT occur */
                }
            }
        }
    }

	public void cancelAllTimers() {
		// TODO Auto-generated method stub
		for (Connection conn : peerConns) {
			cancelTimersFor(conn.getReceiverID());
		}
	}
}
