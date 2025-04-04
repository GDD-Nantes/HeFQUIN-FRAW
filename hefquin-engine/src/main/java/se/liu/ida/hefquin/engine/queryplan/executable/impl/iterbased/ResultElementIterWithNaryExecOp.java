package se.liu.ida.hefquin.engine.queryplan.executable.impl.iterbased;

import se.liu.ida.hefquin.engine.queryplan.executable.ExecutablePlanStats;
import se.liu.ida.hefquin.engine.queryplan.executable.NaryExecutableOp;
import se.liu.ida.hefquin.engine.queryproc.ExecutionContext;
import se.liu.ida.hefquin.engine.queryproc.ExecutionException;

import java.util.List;

public class ResultElementIterWithNaryExecOp extends ResultElementIterBase
{
	protected final MyOpRunnerThread opRunnerThread;

	public ResultElementIterWithNaryExecOp( final NaryExecutableOp op,
											  final List<ResultBlockIterator> inputIters,
											  final ExecutionContext execCxt )
	{
		super(execCxt);

		assert op != null;
		assert inputIters != null;
		assert inputIters.size() > 0;
		assert inputIters.size() > 2;
		assert execCxt != null;

		opRunnerThread = new MyOpRunnerThread( op, inputIters );
	}

	@Override
	public NaryExecutableOp getOp() {
		return opRunnerThread.getOp();
	}

	public ExecutablePlanStats tryGetStatsOfInputN(int n) {
		return ResultIteratorUtils.tryGetStatsOfProducingSubPlan( opRunnerThread.getInputN(n) );
	}

	public List<Exception> tryGetExceptionsOfInputN(int n) {
		return ResultIteratorUtils.tryGetExceptionsOfProducingSubPlan( opRunnerThread.getInputN(n) );
	}

	@Override
	protected OpRunnerThread getOpRunnerThread() {
		return opRunnerThread;
	}


	protected class MyOpRunnerThread extends OpRunnerThread
	{
		private final NaryExecutableOp op;
		protected final List<ResultBlockIterator> inputIters;

		public MyOpRunnerThread( final NaryExecutableOp op,
								 List<ResultBlockIterator> inputIters )
		{
			this.op = op;
			this.inputIters = inputIters;
		}

		@Override
		public NaryExecutableOp getOp() { return op; }

		public ResultBlockIterator getInputN(int n) { return inputIters.get(n); }

		@Override
		protected void _run() throws ExecutionException {
			// Note, we do not need to check op.requiresCompleteChild1InputFirst()
			// here because this implementation is anyways sending the complete
			// intermediate result from input one first, before moving on to
			// input two.

			for( int i = 0; i < inputIters.size(); i++ ) {
				ResultBlockIterator it = inputIters.get(i);
				while ( it.hasNext() ) {
					op.processBlockFromXthChild( i, it.next(), sink, execCxt );
				}
				op.wrapUpForXthChild( i, sink, execCxt);
			}
		}

	} // end of class OpRunnerThread

}