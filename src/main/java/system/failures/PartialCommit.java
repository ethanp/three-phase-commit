package system.failures;

/**
 * Ethan Petuchowski 3/3/15
 */
public class PartialCommit extends Failure {

    final int lastProcID;

    public PartialCommit(int lastProcID) {
        super(Case.PARTIAL_COMMIT);
        this.lastProcID = lastProcID;
    }

    public int getLastProcID() {
        return lastProcID;
    }
}
