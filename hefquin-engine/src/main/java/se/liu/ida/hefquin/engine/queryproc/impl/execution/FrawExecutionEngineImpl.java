package se.liu.ida.hefquin.engine.queryproc.impl.execution;

import se.liu.ida.hefquin.engine.queryplan.executable.ExecutablePlan;
import se.liu.ida.hefquin.engine.queryproc.ExecutionEngine;
import se.liu.ida.hefquin.engine.queryproc.ExecutionException;
import se.liu.ida.hefquin.engine.queryproc.ExecutionStats;
import se.liu.ida.hefquin.engine.queryproc.QueryResultSink;

public class FrawExecutionEngineImpl implements ExecutionEngine {
    // An exeuction engine designed to execute federated aggregation queries
    // Queries processed by this engine should be of the shape VALUES SERVICE

    @Override
    public ExecutionStats execute(ExecutablePlan plan, QueryResultSink resultSink) throws ExecutionException {
        plan.run(resultSink);
        return null;

    }
}
