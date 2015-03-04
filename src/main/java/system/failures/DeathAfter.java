package system.failures;

/**
 * Ethan Petuchowski 3/3/15
 */
public class DeathAfter extends Failure {
    public DeathAfter(int numMsgs, int procID) {
        super(Case.DEATH_AFTER);
        this.numMsgs = numMsgs;
        this.procID = procID;
    }

    final int numMsgs;
    final int procID;

}
