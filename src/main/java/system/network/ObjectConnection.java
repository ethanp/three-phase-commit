package system.network;

import messages.Message;
import messages.NodeMessage;
import system.ManagerNodeRef;
import system.TransactionManager;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Ethan Petuchowski 2/26/15
 */
public class ObjectConnection extends Connection {
    public ObjectInputStream in;
    public ObjectOutputStream out;

    public ObjectConnection(Socket socket, int nodeID) {
        super(nodeID);
        try {
            out = new ObjectOutputStream(socket.getOutputStream());
            in = new ObjectInputStream(socket.getInputStream());
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override public Message receiveMessage() {
        try {
            return (Message) in.readObject();
        }
        catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override public void sendMessage(Message o) {
        try {
            out.writeObject(o);
            out.flush();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static ObjectConnection txnMgrAddConnection(Socket socket, TransactionManager txnMgr) {
        ObjectConnection conn = new ObjectConnection(socket, -1);

        NodeMessage n = (NodeMessage) conn.receiveMessage();
        int nodeID = n.getNodeID();
        int listenPort = n.getListenPort();

        conn.setReceiverID(nodeID);
        ManagerNodeRef mgrNodeRef = txnMgr.remoteNodeWithID(nodeID);
        mgrNodeRef.setListenPort(listenPort);
        mgrNodeRef.setConn(conn);

        return conn;
    }
}
