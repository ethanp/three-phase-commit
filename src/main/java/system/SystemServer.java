package system;

import system.network.ObjectConnection;
import util.Common;

import java.io.IOException;
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
                system.addConn(new ObjectConnection(socket));
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
