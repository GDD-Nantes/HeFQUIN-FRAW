package se.liu.ida.hefquin.engine.queryplan.executable.impl.iterbased;

import se.liu.ida.hefquin.base.data.SolutionMapping;
import se.liu.ida.hefquin.base.data.impl.SolutionMappingImpl;
import se.liu.ida.hefquin.engine.queryplan.executable.ExecOpExecutionException;
import se.liu.ida.hefquin.engine.queryplan.executable.NullaryExecutableOp;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.SynchronizedIntermediateResultElementSink;
import se.liu.ida.hefquin.engine.queryproc.ExecutionContext;
import se.liu.ida.hefquin.engine.queryproc.ExecutionException;

public class SamplingResultElementIterWithNullaryExecOp extends SamplingResultElementIterBase<NullaryExecutableOp> implements StatsProvidingResultElementIterator
{
	protected final NullaryExecutableOp op;
	protected OpRunnerThread opRunnerThread;
	SynchronizedIntermediateResultElementSink sink;

	// testing purposes
	protected int numberOfThreadWakeUps = 0;
	protected int numberOfNexts = 0;

	public SamplingResultElementIterWithNullaryExecOp(final NullaryExecutableOp op,
                                                      final ExecutionContext execCxt )
	{
		super(execCxt);

		assert op != null;
		assert execCxt != null;

		this.op = op;
		this.sink = new SynchronizedIntermediateResultElementSink();
		createNewOpRunnerThread();
	}

	@Override
	public NullaryExecutableOp getOp() {
		return this.op;
	}

	@Override
	public boolean hasNext() {

		if ( nextElement != null ) {
			return true;
		}

		ensureOpRunnerThreadIsStarted();
		ensureOpRunnerThreadHasNoException();

		nextElement = this.getNextElement();

		if(nextElement == null){

			createNewOpRunnerThread();

			sink.open();

			ensureOpRunnerThreadIsStarted();
			ensureOpRunnerThreadHasNoException();

			nextElement = this.getNextElement();

			if(nextElement == null) {
				nextElement = new SolutionMappingImpl();
			}
		}

		ensureOpRunnerThreadHasNoException();

		return true;
	}

	synchronized public SolutionMapping getNextElement() {
		return sink.getNextElement();
	}

	protected void ensureOpRunnerThreadIsStarted() {
		final OpRunnerThread opRunnerThread = getOpRunnerThread();
		if ( opRunnerThread.getState() == Thread.State.NEW ) {
			opRunnerThread.start();
		}
	}

	protected void ensureOpRunnerThreadHasNoException() throws ResultElementIterException {
		final ExecutionException possibleExptn = opRunnerThread.getExceptionIfAny();
		if ( possibleExptn != null ) {
			throw new ResultElementIterException(possibleExptn);
		}
	}

	@Override
	protected void _run() throws ExecutionException {
		throw new ExecutionException("_run() on ResultElementIterWithNullaryExecOp should not be called");
	}

	@Override
	public SolutionMapping next(){
		numberOfNexts++;
		return super.next();
	}

	protected OpRunnerThread getOpRunnerThread() {
		return opRunnerThread;
	}

	protected OpRunnerThread createNewOpRunnerThread() {
		this.opRunnerThread = new OpRunnerThread(op);
		this.numberOfThreadWakeUps++;
		return this.opRunnerThread;
	}

	@Override
	public int getNumberOfNexts() {
		return numberOfNexts;
	}

	@Override
	public int getNumberOfThreadWakeUps() {
		return numberOfThreadWakeUps;
	}

	protected class OpRunnerThread extends Thread
	{
		private final NullaryExecutableOp op;
		ExecutionException ex = null;

		public ExecutionException getExceptionIfAny() { return ex; }

		public OpRunnerThread( final NullaryExecutableOp op ) {
			this.op = op;
		}

		public NullaryExecutableOp getOp() {
			return op;
		}

		@Override
		public void run() {
			try {
				op.execute(sink, execCxt);
			} catch (ExecOpExecutionException e) {
				ex = e;
			}

			sink.close();
		}

	} // end of class OpRunnerThread
}
