package se.liu.ida.hefquin.engine.queryplan.executable.impl.iterbased;

import se.liu.ida.hefquin.base.data.SolutionMapping;
import se.liu.ida.hefquin.engine.queryplan.executable.ExecutablePlanStats;
import se.liu.ida.hefquin.engine.queryproc.ExecutionException;
import se.liu.ida.hefquin.engine.queryproc.QueryResultSink;
import se.liu.ida.hefquin.base.data.utils.Budget;

import java.util.List;

public class IteratorBasedExecutableSamplingPlanImpl extends IteratorBasedExecutablePlanImpl
{
	private final Budget budget;
	private final List<SamplingResultElementIterWithNullaryExecOp> leaves;

	public IteratorBasedExecutableSamplingPlanImpl(final ResultElementIterator it) {
        super(it);
        throw new UnsupportedOperationException("Can't instantiate IteratorBasedExecutableSamplingPlanImpl without a budget");
	}

	public IteratorBasedExecutableSamplingPlanImpl(final ResultElementIterator it, final Budget budget,
												   final List<SamplingResultElementIterWithNullaryExecOp> leaves ) {
		super(it);
		this.budget = budget;
		this.leaves = leaves;
	}

	public void runWithBudget( final QueryResultSink resultSink ) throws ExecutionException {
		try {
			int attempted = 0;
			int results = 0;
			final long start = System.currentTimeMillis();
			while ( it.hasNext() && !shouldStop(attempted, results, start) ) {
				SolutionMapping sm = it.next();
				resultSink.send( it.next() );
				attempted++;
				if(!sm.asJenaBinding().isEmpty()) results++;
			}
			leaves.forEach(SamplingResultElementIterWithNullaryExecOp::flush);
		}
		catch ( final ResultElementIterException ex ) {
			throw new ExecutionException( "An exception occurred during result iteration.", ex.getWrappedExecutionException() );
		}
	}

	private boolean shouldStop(final int attempted, final int results, final long start) {
		return attempted >= budget.getAttempts()
				|| System.currentTimeMillis() - start >= budget.getTimeout()
				|| results >= budget.getLimit();
	}

	@Override
	public void resetStats() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ExecutablePlanStats getStats() {
		return ResultIteratorUtils.tryGetStatsOfProducingSubPlan(it);
	}

	@Override
	public List<Exception> getExceptionsCaughtDuringExecution() {
		return ResultIteratorUtils.tryGetExceptionsOfProducingSubPlan(it);
	}

}
