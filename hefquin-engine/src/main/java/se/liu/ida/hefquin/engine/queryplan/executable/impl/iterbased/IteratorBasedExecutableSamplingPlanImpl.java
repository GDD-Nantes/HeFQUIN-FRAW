package se.liu.ida.hefquin.engine.queryplan.executable.impl.iterbased;

import se.liu.ida.hefquin.engine.queryplan.executable.ExecutablePlanStats;
import se.liu.ida.hefquin.engine.queryproc.ExecutionException;
import se.liu.ida.hefquin.engine.queryproc.QueryResultSink;

import java.util.List;

import static se.liu.ida.hefquin.jenaintegration.sparql.FrawConstants.DEFAULT_RANDOM_WALK;

public class IteratorBasedExecutableSamplingPlanImpl extends IteratorBasedExecutablePlanImpl
{
	private final int numberOfWalks;
	private final int DEFAULT_TIMEOUT_MS = Integer.MAX_VALUE;

	public IteratorBasedExecutableSamplingPlanImpl(final ResultElementIterator it) {
        super(it);
		this.numberOfWalks = DEFAULT_RANDOM_WALK;
	}

	public IteratorBasedExecutableSamplingPlanImpl( final ResultElementIterator it, final int numberOfWalks ) {
		super(it);
		this.numberOfWalks = numberOfWalks;
	}

	public void runWithBudget( final QueryResultSink resultSink ) throws ExecutionException {
		try {
			int attempted = 0;
			final long start = System.currentTimeMillis();
			while ( it.hasNext() && !shouldStop(attempted, start) ) {
				resultSink.send( it.next() );
				attempted++;
			}
		}
		catch ( final ResultElementIterException ex ) {
			throw new ExecutionException( "An exception occurred during result iteration.", ex.getWrappedExecutionException() );
		}
	}

	private boolean shouldStop(final int attempted, final long start) {
		return attempted >= numberOfWalks || System.currentTimeMillis() - start >= DEFAULT_TIMEOUT_MS;
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
