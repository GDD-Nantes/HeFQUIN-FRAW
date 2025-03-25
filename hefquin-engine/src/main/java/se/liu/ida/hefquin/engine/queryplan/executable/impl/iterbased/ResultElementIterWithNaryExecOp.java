package se.liu.ida.hefquin.engine.queryplan.executable.impl.iterbased;

import se.liu.ida.hefquin.engine.queryplan.executable.ExecutablePlanStats;
import se.liu.ida.hefquin.engine.queryplan.executable.NaryExecutableOp;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ops.ExecOpFrawMultiwayUnion;
import se.liu.ida.hefquin.engine.queryproc.ExecutionContext;
import se.liu.ida.hefquin.engine.queryproc.ExecutionException;

import java.util.List;
import java.util.Random;

public class ResultElementIterWithNaryExecOp extends ResultElementIterBase<NaryExecutableOp>
{
	protected final NaryExecutableOp op;
	protected final List<ResultBlockIterator> inputIters;

	public ResultElementIterWithNaryExecOp(final NaryExecutableOp op,
                                           final List<ResultBlockIterator> inputIters,
                                           final ExecutionContext execCxt )
	{
		super(execCxt);

		assert op != null;
		assert inputIters != null;
		assert inputIters.size() > 0;
		assert inputIters.size() > 2;
		assert execCxt != null;

		this.op = op;
		this.inputIters = inputIters;
		createNewOpRunnerThread();
	}

	@Override
	public NaryExecutableOp getOp() {
		return opRunnerThread.getOp();
	}

	public ExecutablePlanStats tryGetStatsOfInputN(int n) {
		return ResultIteratorUtils.tryGetStatsOfProducingSubPlan( ((MyOpRunnerThread) opRunnerThread).getInputN(n) );
	}

	public List<Exception> tryGetExceptionsOfInput1(int n) {
		return ResultIteratorUtils.tryGetExceptionsOfProducingSubPlan( ((MyOpRunnerThread) opRunnerThread).getInputN(n) );
	}

	@Override
	protected OpRunnerThread getOpRunnerThread() {
		return opRunnerThread;
	}

	@Override
	protected OpRunnerThread createNewOpRunnerThread() {
		this.opRunnerThread = new MyOpRunnerThread(op, inputIters);
		return this.opRunnerThread;
	}


	protected class MyOpRunnerThread extends OpRunnerThread
	{
		private final NaryExecutableOp op;
		protected final List<ResultBlockIterator> inputIters;

		public MyOpRunnerThread( final NaryExecutableOp op,
		                         final List<ResultBlockIterator> inputIters)
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

			if(op instanceof ExecOpFrawMultiwayUnion) {
				runFrawMultiwayUnion();
			} else {
				for(int i = 0; i < inputIters.size(); i++) {
					ResultBlockIterator it = inputIters.get(i);
					while ( it.hasNext() ) {
						op.processBlockFromXthChild( i, it.next(), sink, execCxt );
					}
					op.wrapUpForXthChild(i, sink, execCxt);
				}
			}
		}

		private void runFrawMultiwayUnion() throws ExecutionException{
			Random random = new Random();
			int chosen = random.nextInt(inputIters.size());
			ResultBlockIterator it = inputIters.get(chosen);
			if(it.hasNext()){
				op.processBlockFromXthChild( chosen, it.next(), sink, execCxt );
			}
		}

	} // end of class OpRunnerThread

}
