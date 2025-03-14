package se.liu.ida.hefquin.engine.queryplan.executable.impl.iterbased;

import se.liu.ida.hefquin.engine.queryplan.executable.BinaryExecutableOp;
import se.liu.ida.hefquin.engine.queryplan.executable.ExecutablePlanStats;
import se.liu.ida.hefquin.engine.queryproc.ExecutionContext;
import se.liu.ida.hefquin.engine.queryproc.ExecutionException;

import java.util.List;

public class ResultElementIterWithBinaryExecOp extends ResultElementIterBase<BinaryExecutableOp>
{
	protected final BinaryExecutableOp op;
	protected final ResultBlockIterator inputIter1;
	protected final ResultBlockIterator inputIter2;

	public ResultElementIterWithBinaryExecOp( final BinaryExecutableOp op,
	                                          final ResultBlockIterator inputIter1,
	                                          final ResultBlockIterator inputIter2,
	                                          final ExecutionContext execCxt )
	{
		super(execCxt);

		assert op != null;
		assert inputIter1 != null;
		assert inputIter2 != null;
		assert execCxt != null;

		System.out.println("WARNING : INSTANTIATING A " + this.getClass().getName());
		this.op = op;
		this.inputIter1 = inputIter1;
		this.inputIter2 = inputIter2;
		createNewOpRunnerThread();
	}

	@Override
	public BinaryExecutableOp getOp() {
		return opRunnerThread.getOp();
	}

	public ExecutablePlanStats tryGetStatsOfInput1() {
		return ResultIteratorUtils.tryGetStatsOfProducingSubPlan( ((MyOpRunnerThread) opRunnerThread).getInput1() );
	}

	public ExecutablePlanStats tryGetStatsOfInput2() {
		return ResultIteratorUtils.tryGetStatsOfProducingSubPlan( ((MyOpRunnerThread) opRunnerThread).getInput2() );
	}

	public List<Exception> tryGetExceptionsOfInput1() {
		return ResultIteratorUtils.tryGetExceptionsOfProducingSubPlan( ((MyOpRunnerThread) opRunnerThread).getInput1() );
	}

	public List<Exception> tryGetExceptionsOfInput2() {
		return ResultIteratorUtils.tryGetExceptionsOfProducingSubPlan( ((MyOpRunnerThread) opRunnerThread).getInput2() );
	}

	@Override
	protected OpRunnerThread getOpRunnerThread() {
		return opRunnerThread;
	}

	@Override
	protected ResultElementIterBase<BinaryExecutableOp>.OpRunnerThread createNewOpRunnerThread() {
		this.opRunnerThread = new MyOpRunnerThread(op, inputIter1, inputIter2);
		return this.opRunnerThread;
	}


	protected class MyOpRunnerThread extends OpRunnerThread
	{
		private final BinaryExecutableOp op;
		protected final ResultBlockIterator inputIter1;
		protected final ResultBlockIterator inputIter2;

		public MyOpRunnerThread( final BinaryExecutableOp op,
		                         final ResultBlockIterator inputIter1,
		                         final ResultBlockIterator inputIter2 )
		{
			this.op = op;
			this.inputIter1 = inputIter1;
			this.inputIter2 = inputIter2;
		}

		@Override
		public BinaryExecutableOp getOp() { return op; }

		public ResultBlockIterator getInput1() { return inputIter1; }

		public ResultBlockIterator getInput2() { return inputIter2; }

		@Override
		protected void _run() throws ExecutionException {
			// Note, we do not need to check op.requiresCompleteChild1InputFirst()
			// here because this implementation is anyways sending the complete
			// intermediate result from input one first, before moving on to
			// input two.

			if ( inputIter1.hasNext() ) {
				op.processBlockFromChild1( inputIter1.next(), sink, execCxt );
			}
			op.wrapUpForChild1(sink, execCxt);

			if ( inputIter2.hasNext() ) {
				op.processBlockFromChild2( inputIter2.next(), sink, execCxt );
			}
			op.wrapUpForChild2(sink, execCxt);
		}

	} // end of class OpRunnerThread

}
