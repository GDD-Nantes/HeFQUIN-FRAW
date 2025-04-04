package se.liu.ida.hefquin.engine.queryplan.executable.impl.iterbased;

import se.liu.ida.hefquin.engine.queryplan.executable.BinaryExecutableOp;
import se.liu.ida.hefquin.engine.queryplan.executable.ExecutablePlanStats;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ops.ExecOpFrawMultiwayUnion;
import se.liu.ida.hefquin.engine.queryproc.ExecutionContext;
import se.liu.ida.hefquin.engine.queryproc.ExecutionException;

import java.util.List;
import java.util.Random;

public class SamplingResultElementIterWithBinaryExecOp extends SamplingResultElementIterBase<BinaryExecutableOp>
{
	protected final BinaryExecutableOp op;
	protected final ResultBlockIterator inputIter1;
	protected final ResultBlockIterator inputIter2;

	public SamplingResultElementIterWithBinaryExecOp(final BinaryExecutableOp op,
                                                     final ResultBlockIterator inputIter1,
                                                     final ResultBlockIterator inputIter2,
                                                     final ExecutionContext execCxt )
	{
		super(execCxt);

		assert op != null;
		assert inputIter1 != null;
		assert inputIter2 != null;
		assert execCxt != null;

		this.op = op;
		this.inputIter1 = inputIter1;
		this.inputIter2 = inputIter2;
	}

	@Override
	public BinaryExecutableOp getOp() {
		return this.op;
	}

	public ExecutablePlanStats tryGetStatsOfInput1() {
		return ResultIteratorUtils.tryGetStatsOfProducingSubPlan( inputIter1 );
	}

	public ExecutablePlanStats tryGetStatsOfInput2() {
		return ResultIteratorUtils.tryGetStatsOfProducingSubPlan( inputIter2 );
	}

	public List<Exception> tryGetExceptionsOfInput1() {
		return ResultIteratorUtils.tryGetExceptionsOfProducingSubPlan( inputIter1 );
	}

	public List<Exception> tryGetExceptionsOfInput2() {
		return ResultIteratorUtils.tryGetExceptionsOfProducingSubPlan( inputIter2 );
	}

	@Override
	protected void _run() throws ExecutionException {
		// Note, we do not need to check op.requiresCompleteChild1InputFirst()
		// here because this implementation is anyways sending the complete
		// intermediate result from input one first, before moving on to
		// input two.

		if(op instanceof ExecOpFrawMultiwayUnion) {
			runFrawBinaryUnion();
		} else {
			throw new ExecutionException( "Unsupported binary executable op : " + op );
		}
	}

	private void runFrawBinaryUnion() throws ExecutionException{
		Random random = new Random();
		int chosen = random.nextInt(2);
		if (chosen == 0) {
			op.processBlockFromChild1(inputIter1.next(), this, execCxt);
		} else {
			op.processBlockFromChild2(inputIter2.next(), this, execCxt);
        }
	}
}
