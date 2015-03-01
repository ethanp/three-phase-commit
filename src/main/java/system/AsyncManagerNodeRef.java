package system;

/**
 * Ethan Petuchowski 2/28/15
 */
public class AsyncManagerNodeRef extends ManagerNodeRef {
    Process process;
    public AsyncManagerNodeRef(int id, Process p) {
        super(id);
        process = p;
    }
}
