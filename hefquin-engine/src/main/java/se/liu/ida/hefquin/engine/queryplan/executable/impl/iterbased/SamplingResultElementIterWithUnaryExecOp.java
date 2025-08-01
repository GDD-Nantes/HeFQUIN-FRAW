package se.liu.ida.hefquin.engine.queryplan.executable.impl.iterbased;

import se.liu.ida.hefquin.engine.queryplan.executable.ExecutablePlanStats;
import se.liu.ida.hefquin.engine.queryplan.executable.UnaryExecutableOp;
import se.liu.ida.hefquin.engine.queryproc.ExecutionContext;
import se.liu.ida.hefquin.engine.queryproc.ExecutionException;

import java.util.List;

public class SamplingResultElementIterWithUnaryExecOp extends SamplingResultElementIterBase<UnaryExecutableOp>
{
	protected final UnaryExecutableOp op;
	protected final ResultElementIterator inputIter;

	public SamplingResultElementIterWithUnaryExecOp(final UnaryExecutableOp op,
                                                    final ResultElementIterator inputIter,
                                                    final ExecutionContext execCxt )
	{
		super(execCxt);

		assert op != null;
		assert inputIter != null;

		this.op = op;
		this.inputIter = inputIter;
	}

	@Override
	public UnaryExecutableOp getOp() {
		return this.op;
	}

	public ExecutablePlanStats tryGetStatsOfInput() {
		return ResultIteratorUtils.tryGetStatsOfProducingSubPlan( inputIter );
	}

	public List<Exception> tryGetExceptionsOfInput() {
		return ResultIteratorUtils.tryGetExceptionsOfProducingSubPlan( inputIter );
	}

	protected void _run() throws ExecutionException {
		if ( inputIter.hasNext() ) {
			op.process( inputIter.next(), this, execCxt );
		}
		op.concludeExecution(this, execCxt);
	}
}
