package system.failures;

import messages.Message.Command;

/**
 * Ethan Petuchowski 3/5/15
 */
public class NodeFailure extends Failure {

    final Role role;
    final Command stage;

    public NodeFailure(Role role, Command command) {
        super(Case.PARTIAL_BROADCAST);
        this.role = role;
        stage = command;
    }

    public Command getStage() {
        return stage;
    }

    public Role getRole() {
        return role;
    }

    public enum Role {
        CASCADING_COORDINATOR,
        PARTICIPANT,
        TOTAL, FUTURE_COORDINATOR
    }
}
