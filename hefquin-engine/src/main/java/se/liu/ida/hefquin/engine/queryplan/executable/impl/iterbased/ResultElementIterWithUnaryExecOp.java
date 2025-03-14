package se.liu.ida.hefquin.engine.queryplan.executable.impl.iterbased;

import se.liu.ida.hefquin.engine.queryplan.executable.ExecutablePlanStats;
import se.liu.ida.hefquin.engine.queryplan.executable.UnaryExecutableOp;
import se.liu.ida.hefquin.engine.queryproc.ExecutionContext;
import se.liu.ida.hefquin.engine.queryproc.ExecutionException;

import java.util.List;

public class ResultElementIterWithUnaryExecOp extends ResultElementIterBase<UnaryExecutableOp>
{
	protected final UnaryExecutableOp op;
	protected final ResultBlockIterator inputIter;

	public ResultElementIterWithUnaryExecOp( final UnaryExecutableOp op,
	                                         final ResultBlockIterator inputIter,
	                                         final ExecutionContext execCxt )
	{
		super(execCxt);

		assert op != null;
		assert inputIter != null;

		this.op = op;
		this.inputIter = inputIter;
		createNewOpRunnerThread();
	}

	@Override
	public UnaryExecutableOp getOp() {
		return opRunnerThread.getOp();
	}

	public ExecutablePlanStats tryGetStatsOfInput() {
		return ResultIteratorUtils.tryGetStatsOfProducingSubPlan( ((MyOpRunnerThread) opRunnerThread).getInput() );
	}

	public List<Exception> tryGetExceptionsOfInput() {
		return ResultIteratorUtils.tryGetExceptionsOfProducingSubPlan( ((MyOpRunnerThread) opRunnerThread).getInput() );
	}

	@Override
	protected OpRunnerThread getOpRunnerThread() {
		return opRunnerThread;
	}

	@Override
	protected OpRunnerThread createNewOpRunnerThread() {
		this.opRunnerThread = new MyOpRunnerThread( op, inputIter );
		return opRunnerThread;
	}


	protected class MyOpRunnerThread extends OpRunnerThread
	{
		private final UnaryExecutableOp op;
		protected final ResultBlockIterator inputIter;

		public MyOpRunnerThread( final UnaryExecutableOp op,
		                         final ResultBlockIterator inputIter )
		{
			this.op = op;
			this.inputIter = inputIter;
		}

		@Override
		public UnaryExecutableOp getOp() {
			return op;
		}

		public ResultBlockIterator getInput() {
			return inputIter;
		}

		@Override
		protected void _run() throws ExecutionException {
			if ( inputIter.hasNext() ) {
				op.process( inputIter.next(), sink, execCxt );
			}
			op.concludeExecution(sink, execCxt);
		}

	} // end of class OpRunnerThread

}
