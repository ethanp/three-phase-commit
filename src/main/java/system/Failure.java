package system;

/**
 * Ethan Petuchowski 2/17/15
 */
public class Failure {
    public enum Model { NONE }
    Model model;
    int value;

    public Failure(Model model, int value) {
        this.model = model;
        this.value = value;
    }

    public static Failure type(Model model) {
        return new Failure(model, -1);
    }
}
