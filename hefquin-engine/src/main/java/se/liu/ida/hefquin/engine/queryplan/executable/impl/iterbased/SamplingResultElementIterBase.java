package se.liu.ida.hefquin.engine.queryplan.executable.impl.iterbased;

import se.liu.ida.hefquin.base.data.SolutionMapping;
import se.liu.ida.hefquin.base.data.impl.SolutionMappingImpl;
import se.liu.ida.hefquin.engine.queryplan.executable.ExecutableOperator;
import se.liu.ida.hefquin.engine.queryplan.executable.IntermediateResultElementSink;
import se.liu.ida.hefquin.engine.queryproc.ExecutionContext;
import se.liu.ida.hefquin.engine.queryproc.ExecutionException;

import java.util.NoSuchElementException;

public abstract class SamplingResultElementIterBase<OpType extends ExecutableOperator> implements ResultElementIterator, IntermediateResultElementSink
{
	protected final ExecutionContext execCxt;

	protected SolutionMapping nextElement = null;

	protected boolean closed = false;

	protected SamplingResultElementIterBase(final ExecutionContext execCxt ) {
		assert execCxt != null;
		this.execCxt = execCxt;
	}

	@Override
	public boolean hasNext() throws ResultElementIterException {
		if ( nextElement != null ) {
			return true;
		}

		try {
			_run();
		} catch (ExecutionException e) {
			throw new RuntimeException(e);
		}

		if ( nextElement == null ) {
			nextElement = new SolutionMappingImpl();
		}

		return true;
	}

	protected abstract void _run() throws ExecutionException;

	@Override
	public SolutionMapping next() throws ResultElementIterException {
		if ( ! hasNext() ) {
			throw new NoSuchElementException();
		}

		final SolutionMapping returnElement = nextElement;
		nextElement = null;
		return returnElement;
	}

	public abstract OpType getOp();

	@Override
	synchronized public void send( final SolutionMapping element ) {
		nextElement = element;
	}
}
