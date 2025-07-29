package org.apache.flink.runtime.scheduler.adaptive.timeline;

public enum TerminatedReason {
    SUCCEEDED(TerminalState.COMPLETED),
    EXCEPTION_OCCURRED(TerminalState.FAILED),
    RESOURCE_REQUIREMENTS_UPDATED(TerminalState.IGNORED),
    NO_RESOURCES_OR_PARALLELISMS_CHANGE(TerminalState.IGNORED),
    JOB_FINISHED(TerminalState.IGNORED),
    JOB_FAILING(TerminalState.IGNORED),
    JOB_FAILOVER_RESTARTING(TerminalState.IGNORED),
    JOB_CANCELING(TerminalState.IGNORED);

    private final TerminalState terminalState;

    TerminatedReason(TerminalState terminalState) {
        this.terminalState = terminalState;
    }

    public TerminalState getTerminalState() {
        return terminalState;
    }
}
