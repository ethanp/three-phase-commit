package console;

import messages.KillSig;
import messages.ListCommand;
import messages.Message;
import messages.vote_req.AddRequest;
import messages.vote_req.DeleteRequest;
import messages.vote_req.UpdateRequest;
import messages.vote_req.VoteRequest;
import system.failures.DeathAfter;
import system.failures.PartialBroadcast;
import util.SongTuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
* Ethan Petuchowski 3/6/15
*/
public class ConsoleCommand {

    VoteRequest voteRequest;
    List<Message> failureModes = new ArrayList<>();
    int delay = -1;

    public ConsoleCommand(String cmdString, int txnID) {
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
                return;
            case "list":
                voteRequest = new ListCommand();
                return;
            default:
                System.err.println("Unrecognized command");
        }

        /* read failure case / option */
        while (cmdSc.hasNext()) {
            switch (cmdSc.next()) {
                case "-partialCommit":
                    failureModes.add(
                            new PartialBroadcast(
                                    Message.Command.COMMIT,
                                    Integer.parseInt(cmdSc.next()),
                                    Integer.parseInt(cmdSc.next())));
                    break;
                case "-partialPrecommit":
                    failureModes.add(
                            new PartialBroadcast(
                                    Message.Command.PRE_COMMIT,
                                    Integer.parseInt(cmdSc.next()),
                                    Integer.parseInt(cmdSc.next())));
                    break;
                case "-deathAfter":
                    failureModes.add(
                            new DeathAfter(
                                    Integer.parseInt(cmdSc.next()),
                                    Integer.parseInt(cmdSc.next()),
                                    Integer.parseInt(cmdSc.next())));
                    break;
                case "-deathAfterElected":
                    failureModes.add(
                            new DeathAfter(
                                    DeathAfter.ELECTION_DEATH,
                                    DeathAfter.ELECTION_DEATH,
                                    Integer.parseInt(cmdSc.next())));
                    break;
                case "-delay":
                    delay = Integer.parseInt(cmdSc.next());
                    break;

                default:
                    System.err.println("Invalid command");
                    voteRequest = null;
                    return;
            }
        }
    }

    public VoteRequest getVoteRequest() {
        return voteRequest;
    }

    public List<Message> getFailureModes() {
        return failureModes;
    }

    public int getDelay() {
        return delay;
    }
}
