package se.liu.ida.hefquin.engine.queryproc.impl.execution;

import se.liu.ida.hefquin.engine.queryplan.executable.ExecutablePlan;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.iterbased.IteratorBasedExecutableSamplingPlanImpl;
import se.liu.ida.hefquin.engine.queryproc.ExecutionEngine;
import se.liu.ida.hefquin.engine.queryproc.ExecutionException;
import se.liu.ida.hefquin.engine.queryproc.ExecutionStats;
import se.liu.ida.hefquin.engine.queryproc.QueryResultSink;

public class FrawExecutionEngineImpl implements ExecutionEngine
{
	@Override
	public ExecutionStats execute( final ExecutablePlan plan, final QueryResultSink resultSink )
			throws ExecutionException
	{
		if(plan instanceof IteratorBasedExecutableSamplingPlanImpl){
			((IteratorBasedExecutableSamplingPlanImpl) plan).runWithBudget(resultSink);
		} else {
			throw new ExecutionException("Unsupported executable plan type: " + plan.getClass().getName());
		}

		return new ExecutionStatsImpl( plan.getStats() );
	}

}
