package system;

/**
 * Ethan Petuchowski 2/17/15
 */
public class Failure {
    public enum Case {
        NONE
    }
    Case aCase;
    int value;

    public Failure(Case aCase, int value) {
        this.aCase = aCase;
        this.value = value;
    }

    public static Failure type(Case aCase) {
        return new Failure(aCase, -1);
    }
}
