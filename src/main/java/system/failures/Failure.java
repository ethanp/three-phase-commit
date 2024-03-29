package system.failures;

/**
 * Ethan Petuchowski 2/17/15
 */
public abstract class Failure {
    public enum Case {
        PARTIAL_BROADCAST, DEATH_AFTER, NONE
    }

    Case theCase;

    public Failure(Case theCase) {
        this.theCase = theCase;
    }

    public Case getTheCase() {
        return theCase;
    }
}
