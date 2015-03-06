package system.failures;

import messages.Message.Command;

/**
 * Ethan Petuchowski 3/3/15
 */
public class PartialBroadcast extends Failure {

    final int lastProcID;
    final Command stage;

    public PartialBroadcast(Command stage, int lastProcID) {
        super(Case.PARTIAL_BROADCAST);
        this.stage = stage;
        this.lastProcID = lastProcID;
    }

    public Command getStage() {
        return stage;
    }

    public int getLastProcID() {
        return lastProcID;
    }
}
