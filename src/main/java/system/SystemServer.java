package system;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import util.Common;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Ethan Petuchowski 2/26/15
 */
public class SystemServer implements Runnable {
    TransactionManager system;
    ServerSocket serverSocket;
    static int requestPort = 3000;

    public int getListenPort() {
        return serverSocket.getLocalPort();
    }

    public SystemServer(TransactionManager system) {
        this.system = system;
        serverSocket = Common.claimOpenPort();
    }

    @Override public void run() {
        while (true) {
            try {
                Socket socket = serverSocket.accept();
                ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                try {
                    int nodeID = (int) ois.readObject();
                    int nodeListenPort = (int) ois.readObject();
                    /* get the node with this id and give it a connection with this info */
                    throw new NotImplementedException();
                }
                catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
