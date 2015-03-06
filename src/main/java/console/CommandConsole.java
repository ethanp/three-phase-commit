package console;

import com.sun.istack.internal.NotNull;
import system.AsyncTxnMgr;

import java.util.Scanner;

/**
 * Ethan Petuchowski 3/3/15
 */
public class CommandConsole implements Runnable {

    Scanner sc = new Scanner(System.in);
    int transactionID = 0;
    final AsyncTxnMgr transactionManager;

    public CommandConsole(@NotNull AsyncTxnMgr mgr) {
        transactionManager = mgr;
    }

    @Override public void run() {
        while (true) {
            String cmdStr = sc.nextLine();
            ConsoleCommand cmd = new ConsoleCommand(cmdStr, ++transactionID);
            transactionManager.processCommand(cmd);
        }
    }
}
