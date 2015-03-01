package node.base;

import node.CoordinatorStateMachine;
import node.ParticipantStateMachine;
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

    public void log(String string) {
        dtLog.log(string);
    }

    public boolean hasSong(SongTuple tuple) {
        return playlist.contains(tuple);
    }

    public boolean hasSong(String name) {
        return playlist.contains(new SongTuple(name, "doesn't matter"));
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

    protected boolean receiveMessageFrom(Connection connection) {
        return stateMachine.receiveMessage(connection);
    }

    public void becomeCoordinator() {
        stateMachine = new CoordinatorStateMachine();
    }

    public StateMachine getStateMachine() {
        return stateMachine;
    }
}
