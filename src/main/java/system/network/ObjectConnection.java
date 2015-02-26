package system.network;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Ethan Petuchowski 2/26/15
 */
public class ObjectConnection {
    ObjectInputStream objIn;
    ObjectOutputStream objOut;
    public ObjectConnection(Socket socket) {
        try {
            objIn = new ObjectInputStream(socket.getInputStream());
            objOut = new ObjectOutputStream(socket.getOutputStream());
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
