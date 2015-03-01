package util;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * Ethan Petuchowski 2/26/15
 */
public class Common {
    public static final int EXIT_FAILURE = 1;
    public static final String LOCALHOST = "127.0.0.1";
    static int reqPort = 3000;
    public static final int TXN_MGR_ID = 0;

    public static ServerSocket claimOpenPort() {
        ServerSocket serverSocket = null;
        while (serverSocket == null) {
            try {
                serverSocket = new ServerSocket(reqPort++);
            }
            catch (IOException e) {
                // ignore

                /* ^ This will cause an infinite loop if the reason we can't claim
                 * a port is NOT that the port is already taken... */
            }
        }
        return serverSocket;
    }
}
