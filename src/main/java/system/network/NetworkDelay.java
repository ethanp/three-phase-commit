package system.network;

/**
 * Ethan Petuchowski 2/17/15
 */
public class NetworkDelay {
    public NetworkDelay(Type type, int val) {
        this.type = type;
        this.value = val;
    }

    public enum Type {
        NONE
    }

    Type type;
    int value;

    public static NetworkDelay type(Type type) {
        return new NetworkDelay(type, -1);
    }

    static NetworkDelay type(Type type, int val) {
        return new NetworkDelay(type, val);
    }
}
