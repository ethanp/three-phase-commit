package node.base;

import messages.Message;
import messages.TokenWriter;
import messages.vote_req.AddRequest;
import messages.vote_req.DeleteRequest;
import messages.vote_req.UpdateRequest;
import messages.vote_req.VoteRequest;
import node.CoordinatorStateMachine;
import node.ParticipantStateMachine;
import node.PeerReference;
import system.network.Connection;
import util.SongTuple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

/**
 * Ethan Petuchowski 2/27/15
 */
public abstract class Node {

    protected StateMachine stateMachine;
    protected final int myNodeID;
    protected final Set<SongTuple> playlist = new TreeSet<>();
    protected DTLog dtLog;

    protected Connection txnMgrConn;

    protected Collection<Connection> peerConns = new ArrayList<>();

    public Node(int myNodeID) {
        this.myNodeID = myNodeID;
        stateMachine = new ParticipantStateMachine(this);
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

    public void logMessage(Message message) {
    	TokenWriter writer = dtLog.beginLog();
        Message.writeMessage(message, writer);
        dtLog.endLog(writer);
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

    public boolean hasSong(String name) {
        return playlist.contains(new SongTuple(name, "doesn't matter"));
    }

    public void commitAction(VoteRequest action) {
        switch (action.getCommand()) {
        case ADD:
            commitAdd((AddRequest) action);
            break;
        case UPDATE:
            commitUpdate((UpdateRequest) action);
            break;
        case DELETE:
            commitDelete((DeleteRequest) action);
            break;
        default:
        	break;
	    }
    }

    private void commitDelete(DeleteRequest deleteRequest) {
        removeSongWithName(deleteRequest.getSongName());
    }

    private void commitUpdate(UpdateRequest updateRequest) {
        updateSong(updateRequest.getSongName(), updateRequest.getUpdatedSong());
    }

    private void commitAdd(AddRequest addRequest) {
        addSong(addRequest.getSongTuple());
    }

    public boolean addSong(SongTuple songTuple) {
        return playlist.add(songTuple);
    }

    public boolean removeSongWithName(String songName) {
        return playlist.remove(new SongTuple(songName, ""));
    }

    public void updateSong(String songName, SongTuple updatedSong) {
        removeSongWithName(songName);
        addSong(updatedSong);
    }

    protected synchronized boolean receiveMessageFrom(Connection connection) {
        return stateMachine.receiveMessage(connection);
    }

    public void becomeCoordinator() {
        stateMachine = new CoordinatorStateMachine(this);
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
        txnMgrConn.sendMessage(message);
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
}
