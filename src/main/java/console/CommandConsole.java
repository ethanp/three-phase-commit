package console;

import com.sun.istack.internal.NotNull;
import messages.vote_req.AddRequest;
import messages.vote_req.DeleteRequest;
import messages.vote_req.UpdateRequest;
import messages.vote_req.VoteRequest;
import system.TransactionManager;
import system.failures.DeathAfter;
import system.failures.Failure;
import system.failures.PartialCommit;
import util.SongTuple;

import java.util.Scanner;

/**
 * Ethan Petuchowski 3/3/15
 */
public class CommandConsole implements Runnable {

    Scanner sc = new Scanner(System.in);
    int transactionID = 0;
    final TransactionManager transactionManager;

    public CommandConsole(@NotNull TransactionManager mgr) {
        transactionManager = mgr;
    }

    @Override public void run() {
        while (true) {
            String cmdStr = sc.nextLine();
            Command cmd = new Command(cmdStr, ++transactionID);
        }
    }

    public static class Command {

        VoteRequest voteRequest;
        Failure failureMode;
        int delay;

        Command(String cmdString, int txnID) {
            Scanner cmdSc = new Scanner(cmdString);

            /* read command info */
            switch (cmdSc.next()) {
                case "add":
                    voteRequest = new AddRequest(new SongTuple(cmdSc.next(), cmdSc.next()), txnID, null);
                    break;
                case "update":
                    voteRequest = new UpdateRequest(cmdSc.next(), new SongTuple(cmdSc.next(), cmdSc.next()), txnID, null);
                    break;
                case "delete":
                    voteRequest = new DeleteRequest(cmdSc.next(), txnID, null);
                    break;
                default:
                    System.err.println("Unrecognized command");
            }

            /* read failure case */
            while (cmdSc.hasNext()) {
                switch (cmdSc.next()) {
                    case "-partialCommit":
                        failureMode = new PartialCommit(Integer.parseInt(cmdSc.next()));
                        break;
                    case "-delay":
                        delay = Integer.parseInt(cmdSc.next());
                        break;
                    case "-deathAfter":
                        failureMode = new DeathAfter(Integer.parseInt(cmdSc.next()), Integer.parseInt(cmdSc.next()));
                        break;
                }
            }
        }

        public VoteRequest getVoteRequest() {
            return voteRequest;
        }

        public Failure getFailureMode() {
            return failureMode;
        }

        public int getDelay() {
            return delay;
        }
    }
}
