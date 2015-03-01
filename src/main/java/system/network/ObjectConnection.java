package system.network;

import messages.Message;

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
            in = new ObjectInputStream(socket.getInputStream());
            out = new ObjectOutputStream(socket.getOutputStream());
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
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
