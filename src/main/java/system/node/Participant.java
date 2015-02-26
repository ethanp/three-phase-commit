package system.node;

import system.Node;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Ethan Petuchowski 2/17/15
 */
public class Participant extends Node {
    {
        protocol = new ParticipantProtocol();
        try (ServerSocket serverSocket = new ServerSocket(portNum)) {
            System.out.println("listing on port: "+serverSocket.getLocalPort());

            for (;;) {
                Socket socket = serverSocket.accept();

            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    ServerSocket serverSocket;

    public Participant(int port) {
        super(port);
    }

    public static void main(String[] args) throws IOException {
        verifyArgs(args);
        int port = Integer.parseInt(args[0]);
        System.out.println("Trying to serve on port: "+args[1]);
        Participant participant = new Participant(port);
        Socket socket = new Socket(LOCALHOST, port);
        System.out.println("Coordinator connected...");
        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
        writer.println("pingTwo 1st\npingTwo 2nd\n");
        String inLine;
        while ((inLine = reader.readLine()) != null && !inLine.trim().isEmpty()) {
            System.out.println("Pinger rcvd: "+inLine);
        }
    }


}
