package system.network;

import messages.Message;
import util.Common;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketException;

/**
 * Ethan Petuchowski 2/26/15
 */
public class ObjectConnection extends Connection {
    public ObjectInputStream in;
    public ObjectOutputStream out;
    public Socket socket;

    public ObjectConnection(Socket socket, int nodeID) {
        super(nodeID);
        this.socket = socket;
        try {
            out = new ObjectOutputStream(socket.getOutputStream());
            in = new ObjectInputStream(socket.getInputStream());
        }
        catch (IOException e) {
            System.err.println("Failed to create object streams to "+getReceiverID());
        }
    }

    @Override public Message receiveMessage() throws EOFException {
        if (isReady()) {
            try {
                return (Message) in.readObject();
            }
            catch (EOFException e) {
                System.err.println("Node "+Common.ASYNC_NODE_ID+": EOF from "+getReceiverID()+", "+
                                   "closing socket");
                try {
                    socket.close();
                }
                catch (IOException e1) {
                    System.err.println("Can't close socket");
                }
            }
            catch (SocketException e) {
                System.err.println("SocketException from "+getReceiverID()+", "+
                                   "should we remove this ObjectConn ?");
            }
            catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        else {
            System.err.println("Socket is closed");
        }
        return null;
    }

    @Override public void sendMessage(Message o) throws IOException {
        out.writeObject(o);
        out.flush();
    }

    public boolean isReady() {
        return socket.isBound()
               && socket.isConnected()
               && !socket.isClosed()
               && in != null
               && out != null;
    }
}
