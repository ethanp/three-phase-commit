package node;

import java.io.Serializable;

/**
 * Ethan Petuchowski 2/27/15
 */
public class PeerReference implements Comparable<PeerReference>, Cloneable, Serializable {
    protected final int nodeID;
    protected int listeningPort;

    public PeerReference(int nodeID, int listeningPort) {
        this.nodeID = nodeID;
        this.listeningPort = listeningPort;
    }

    public int getNodeID() {
        return nodeID;
    }

    public int getListeningPort() {
        return listeningPort;
    }

    public void setListeningPort(int listeningPort) {
        this.listeningPort = listeningPort;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PeerReference)) return false;
        PeerReference reference = (PeerReference) o;
        if (listeningPort != reference.listeningPort) return false;
        if (nodeID != reference.nodeID) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = nodeID;
        result = 31*result+listeningPort;
        return result;
    }

    @Override public int compareTo(PeerReference o) {
        return getNodeID()-o.getNodeID();
    }

    public PeerReference clone() {
        return new PeerReference(getNodeID(), getListeningPort());
    }
}
