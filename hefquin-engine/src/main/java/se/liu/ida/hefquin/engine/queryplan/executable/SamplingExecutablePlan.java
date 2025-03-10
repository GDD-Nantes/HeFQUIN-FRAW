package se.liu.ida.hefquin.engine.queryplan.executable;

import se.liu.ida.hefquin.engine.queryproc.QueryResultSink;

public interface SamplingExecutablePlan extends ExecutablePlan{
    public void runForNWalks(QueryResultSink resultSink, int numberOfRandomWalksToAttempt);
}
