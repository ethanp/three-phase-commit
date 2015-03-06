package console;

import com.sun.istack.internal.NotNull;
import messages.KillSig;
import messages.Message;
import messages.vote_req.AddRequest;
import messages.vote_req.DeleteRequest;
import messages.vote_req.UpdateRequest;
import messages.vote_req.VoteRequest;
import system.AsyncTxnMgr;
import system.failures.DeathAfter;
import system.failures.Failure;
import system.failures.PartialBroadcast;
import util.SongTuple;

import java.util.ArrayList;
import java.util.List;
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
            Command cmd = new Command(cmdStr, ++transactionID);
            transactionManager.processCommand(cmd);
        }
    }

    public static class Command {

        VoteRequest voteRequest;
        List<Failure> failureMode = new ArrayList<>();
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
                case "kill":
                    voteRequest = new KillSig(Integer.parseInt(cmdSc.next()));
                    break;
                default:
                    System.err.println("Unrecognized command");
            }

            /* read failure case / option */
            while (cmdSc.hasNext()) {
                switch (cmdSc.next()) {
                    case "-partialCommit":
                        failureMode.add(new PartialBroadcast(Message.Command.COMMIT, Integer.parseInt(cmdSc.next())));
                        break;
                    case "-partialPrecommit":
                        failureMode.add(new PartialBroadcast(Message.Command.PRE_COMMIT, Integer.parseInt(cmdSc.next())));
                        break;
                    case "-deathAfter":
                        failureMode.add(new DeathAfter(Integer.parseInt(cmdSc.next()), Integer.parseInt(cmdSc.next())));
                        break;
                    case "-delay":
                        delay = Integer.parseInt(cmdSc.next());
                        break;

                    /* if there's time */
//                    case "-participantFailure": // e.g. we can have it fail before PRE_COMMIT
//                        failureMode = new NodeFailure(Role.PARTICIPANT, Message.parseCommand(cmdSc.next())));
//                        break;
//                    case "-futureCoordinatorFailure":
//                        failureMode = new NodeFailure(Role.FUTURE_COORDINATOR, Message.parseCommand(cmdSc.next())));
//                        break;
//                    case "-cascadingCoordinatorFailure":
//                        failureMode = new NodeFailure(Role.CASCADING_COORDINATOR, Message.parseCommand(cmdSc.next())));
//                        break;
//                    case "-totalFailure":
//                        failureMode = new NodeFailure(Role.TOTAL, Message.parseCommand(cmdSc.next())));
//                        break;
                }
            }
        }

        public VoteRequest getVoteRequest() {
            return voteRequest;
        }

        public List<Failure> getFailureMode() {
            return failureMode;
        }

        public int getDelay() {
            return delay;
        }
    }
}
