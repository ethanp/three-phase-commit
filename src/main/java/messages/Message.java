package messages;

import messages.vote_req.AddRequest;
import messages.vote_req.DeleteRequest;
import messages.vote_req.UpdateRequest;

import java.io.Serializable;

/**
 * Ethan Petuchowski 2/17/15
 */
public abstract class Message implements Serializable {
    public Message(Command command, int transactionID) {
        this.command = command;
        this.transactionID = transactionID;
    }

    public Message(Command command) {
        this(command, -1);
    }

    public Command getCommand() {
        return command;
    }

    public static Command parseCommand(String word) {
        for (Command command : Command.values()) {
            if (word.equalsIgnoreCase(command.toString())) {
                return command;
            }
        }
        return Command.NONE;
    }

    public enum Command {
        NONE,
        DUB_COORDINATOR,
        PRE_COMMIT,
        COMMIT,
        ABORT,
        YES,
        NO,
        ACK,
        ADD,
        UPDATE,
        DELETE,
        UR_ELECTED,
        TIMEOUT,
        NODE,
        DECISION_REQUEST,
        STATE_REQUEST,
        UNCERTAIN,
        KILL_SIG,
        IN_RECOVERY,
        PARTIAL_BROADCAST,
        DELAY, DEATH_AFTER
    }
    Command command;

    public int getTransactionID() {
        return transactionID;
    }

    protected int transactionID;

    protected abstract void writeAsTokens(TokenWriter writer);
    protected abstract void readFromTokens(TokenReader reader);

    public static void writeMessage(Message m, TokenWriter writer) {
    	writer.writeToken(m.command.toString());
    	m.writeAsTokens(writer);
    }

    public static Message readMessage(TokenReader reader) {
    	String commandString = reader.readToken();
    	if (commandString == null) {
    		return null;
    	}
    	Command command = Command.valueOf(commandString);
    	Message m = null;
    	switch (command) {
		case ABORT:
			m = new AbortRequest(-1);
			break;
		case ACK:
			m = new AckRequest(-1);
			break;
		case ADD:
			m = new AddRequest(null, -1, null);
			break;
		case COMMIT:
			m = new CommitRequest(-1);
			break;
		case DELETE:
			m = new DeleteRequest(null, -1, null);
			break;
		case DUB_COORDINATOR:
			m = new DubCoordinatorMessage();
			break;
		case NO:
			m = new NoResponse(null);
			break;
		case PRE_COMMIT:
			m = new PrecommitRequest(-1);
			break;
		case UPDATE:
			m = new UpdateRequest(null, null, -1, null);
			break;
		case UR_ELECTED:
			m = new ElectedMessage(-1);
			break;
		case YES:
			m = new YesResponse(null);
			break;
		case TIMEOUT:
			m = new PeerTimeout(-1);
			break;
		default:
			throw new RuntimeException("Cannot read message from tokens");
    	}
    	m.readFromTokens(reader);
    	return m;
    }
}
