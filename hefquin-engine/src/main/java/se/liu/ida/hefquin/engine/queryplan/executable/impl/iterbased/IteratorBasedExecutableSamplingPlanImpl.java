package se.liu.ida.hefquin.engine.queryplan.executable.impl.iterbased;

import se.liu.ida.hefquin.engine.queryplan.executable.ExecutablePlanStats;
import se.liu.ida.hefquin.engine.queryproc.ExecutionException;
import se.liu.ida.hefquin.engine.queryproc.QueryResultSink;

import java.util.List;

public class IteratorBasedExecutableSamplingPlanImpl extends IteratorBasedExecutablePlanImpl
{
	private final int DEFAULT_NUMBER_OF_WALKS = 100;
	private final int DEFAULT_TIMEOUT_MS = Integer.MAX_VALUE;

	public IteratorBasedExecutableSamplingPlanImpl(final ResultElementIterator it ) {
        super(it);
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
		return attempted >= DEFAULT_NUMBER_OF_WALKS || System.currentTimeMillis() - start >= DEFAULT_TIMEOUT_MS;
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
