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

    public static ServerSocket claimOpenPort() {
        ServerSocket serverSocket = null;
        while (serverSocket == null) {
            try {
                serverSocket = new ServerSocket(reqPort++);
            }
            catch (IOException e) {
                // ignore
            }
        }
        return serverSocket;
    }
}
