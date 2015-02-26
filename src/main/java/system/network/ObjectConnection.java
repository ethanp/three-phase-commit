package system.network;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Ethan Petuchowski 2/26/15
 */
public class ObjectConnection {
    public ObjectInputStream in;
    public ObjectOutputStream out;
    public ObjectConnection(Socket socket) {
        try {
            in = new ObjectInputStream(socket.getInputStream());
            out = new ObjectOutputStream(socket.getOutputStream());
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
