package se.liu.ida.hefquin.engine.queryplan.executable.impl.iterbased;

import se.liu.ida.hefquin.engine.queryplan.executable.ExecutablePlanStats;
import se.liu.ida.hefquin.engine.queryplan.executable.NaryExecutableOp;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ops.ExecOpFrawMultiwayUnion;
import se.liu.ida.hefquin.engine.queryproc.ExecutionContext;
import se.liu.ida.hefquin.engine.queryproc.ExecutionException;

import java.util.List;
import java.util.Random;

public class SamplingResultElementIterWithNaryExecOp extends SamplingResultElementIterBase<NaryExecutableOp>
{
	protected final NaryExecutableOp op;
	protected final List<ResultBlockIterator> inputIters;

	public SamplingResultElementIterWithNaryExecOp(final NaryExecutableOp op,
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
	}

	public ExecutablePlanStats tryGetStatsOfInputN(int n) {
		return ResultIteratorUtils.tryGetStatsOfProducingSubPlan( inputIters.get(n) );
	}

	public List<Exception> tryGetExceptionsOfInput1(int n) {
		return ResultIteratorUtils.tryGetExceptionsOfProducingSubPlan( inputIters.get(n) );
	}

	@Override
	protected void _run() throws ExecutionException {

		if(op instanceof ExecOpFrawMultiwayUnion) {
			runFrawMultiwayUnion();
		} else {
			throw new ExecutionException("Unsupported operation : " + op.getClass().getName());
		}
	}

	@Override
	public NaryExecutableOp getOp() {
		return this.op;
	}

	private void runFrawMultiwayUnion() throws ExecutionException{
		Random random = new Random();
		int chosen = random.nextInt(inputIters.size());
		ResultBlockIterator it = inputIters.get(chosen);
		if(it.hasNext()){
			op.processBlockFromXthChild( chosen, it.next(), this, execCxt );
		}
	}
}
