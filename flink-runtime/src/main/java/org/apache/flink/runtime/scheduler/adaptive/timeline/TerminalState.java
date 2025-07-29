package org.apache.flink.runtime.scheduler.adaptive.timeline;

public enum TerminalState {
    COMPLETED,
    FAILED,
    IGNORED;

    public static boolean isTerminated(TerminalState terminalState) {
        return terminalState != null;
    }
}
