package se.liu.ida.hefquin.engine.queryplan.executable.impl.iterbased;

import se.liu.ida.hefquin.engine.queryplan.executable.NullaryExecutableOp;
import se.liu.ida.hefquin.engine.queryproc.ExecutionContext;
import se.liu.ida.hefquin.engine.queryproc.ExecutionException;

public class ResultElementIterWithNullaryExecOp extends ResultElementIterBase
{
	protected MyOpRunnerThread opRunnerThread;
	protected final NullaryExecutableOp op;

	public ResultElementIterWithNullaryExecOp( final NullaryExecutableOp op,
	                                           final ExecutionContext execCxt )
	{
		super(execCxt);

		assert op != null;
		assert execCxt != null;

		opRunnerThread = new MyOpRunnerThread(op);
		this.op = op;
	}

	@Override
	public NullaryExecutableOp getOp() {
		return opRunnerThread.getOp();
	}

	@Override
	protected OpRunnerThread getOpRunnerThread() {
		return opRunnerThread;
	}

	@Override
	public boolean hasNext() throws ResultElementIterException {
		// From here to
		ensureOpRunnerThreadIsStarted();
		ensureOpRunnerThreadHasNoException();

		if(sink.isClosed()){
			sink.open();
			this.opRunnerThread = new MyOpRunnerThread(op);
			ensureOpRunnerThreadIsStarted();
		}

		nextElement = sink.getNextElement();

		ensureOpRunnerThreadHasNoException();

		return true;
	}

	protected class MyOpRunnerThread extends OpRunnerThread
	{
		private final NullaryExecutableOp op;

		public MyOpRunnerThread( final NullaryExecutableOp op ) {
			this.op = op;
		}

		@Override
		public NullaryExecutableOp getOp() {
			return op;
		}

		@Override
		protected void _run() throws ExecutionException {
			op.execute(sink, execCxt);
		}

	} // end of class OpRunnerThread
	
}
