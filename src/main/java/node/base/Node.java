package node.base;

import messages.Message;
import node.ParticipantStateMachine;
import system.Protocol;
import util.SongTuple;

import java.util.Set;
import java.util.TreeSet;

/**
 * Ethan Petuchowski 2/27/15
 */
public abstract class Node {

    protected StateMachine stateMachine;
    Protocol protocol;
    protected int myNodeID;
    final Set<SongTuple> playlist = new TreeSet<>();
    protected DTLog dtLog;

    public Node(int myNodeID) {
        this.myNodeID = myNodeID;
        stateMachine = new ParticipantStateMachine(this);
    }

    public int getMyNodeID() {
        return myNodeID;
    }

    public void setMyNodeID(int myNodeID) {
        this.myNodeID = myNodeID;
    }

    public DTLog getDtLog() {
        return dtLog;
    }

    public void setDtLog(DTLog dtLog) {
        this.dtLog = dtLog;
    }

    public abstract void sendMessage(Message message);

    public void log(String string) {
        dtLog.log(string);
    }

    public boolean hasSong(SongTuple tuple) {
        return playlist.contains(tuple);
    }

    public void log(Message msg) {
        log(msg.getTransactionID()+" "+msg.getCommand());
    }

    public boolean hasSong(String name) {
        return playlist.contains(new SongTuple(name, "doesn't matter"));
    }

    public boolean addSong(SongTuple songTuple) {
        return playlist.add(songTuple);
    }
}
