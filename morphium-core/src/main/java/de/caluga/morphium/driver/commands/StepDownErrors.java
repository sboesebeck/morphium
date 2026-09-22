package de.caluga.morphium.driver.commands;

import de.caluga.morphium.driver.MorphiumDriverException;

/**
 * The server answers that say "this node is not (or no longer) the primary": a stepdown, a
 * priority takeover, a shutdown, or - PoppyDB - a node re-syncing after a leader change. A
 * command rejected with one of them has not been executed, so it can be sent again to the
 * re-resolved primary. Shared by the write path ({@link WriteMongoCommand}) and the read path
 * ({@link ReadMongoCommand}, #393).
 */
public final class StepDownErrors {

    /** mongod: NotWritablePrimary - also PoppyDB's answer to a write on a secondary */
    public static final int NOT_WRITABLE_PRIMARY = 10107;
    /** mongod: PrimarySteppedDown */
    public static final int PRIMARY_STEPPED_DOWN = 189;
    /** mongod: ShutdownInProgress */
    public static final int SHUTDOWN_IN_PROGRESS = 91;
    /** mongod: InterruptedAtShutdown */
    public static final int INTERRUPTED_AT_SHUTDOWN = 11600;
    /** mongod: InterruptedDueToReplStateChange - an operation killed by the stepdown itself */
    public static final int INTERRUPTED_DUE_TO_REPL_STATE_CHANGE = 11602;
    /** mongod: NotPrimaryNoSecondaryOk - a primary read on a node that is not primary; PoppyDB sends it as well */
    public static final int NOT_PRIMARY_NO_SECONDARY_OK = 13435;
    /** mongod: NotPrimaryOrSecondary (RECOVERING); PoppyDB: "node is recovering", a secondary running its re-sync */
    public static final int NOT_PRIMARY_OR_SECONDARY = 13436;

    private StepDownErrors() {
    }

    /**
     * @param e the exception a command was answered with
     * @return true when the node rejected the command because it is not the primary (see the
     *         codes above), or - for servers that send no code - says so in its message
     */
    public static boolean isStepDownError(MorphiumDriverException e) {
        if (e.getMongoCode() instanceof Number mc) {
            switch (mc.intValue()) {
                case NOT_WRITABLE_PRIMARY:
                case PRIMARY_STEPPED_DOWN:
                case SHUTDOWN_IN_PROGRESS:
                case INTERRUPTED_AT_SHUTDOWN:
                case INTERRUPTED_DUE_TO_REPL_STATE_CHANGE:
                case NOT_PRIMARY_NO_SECONDARY_OK:
                case NOT_PRIMARY_OR_SECONDARY:
                    return true;
                default:
                    break;
            }
        }
        String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
        return msg.contains("not primary") || msg.contains("not master");
    }
}
