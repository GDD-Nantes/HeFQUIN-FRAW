package se.liu.ida.hefquin.engine.queryproc.impl.compiler;

import se.liu.ida.hefquin.base.query.ExpectedVariables;
import se.liu.ida.hefquin.engine.queryplan.executable.*;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.iterbased.*;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalPlan;
import se.liu.ida.hefquin.engine.queryproc.ExecutionContext;
import se.liu.ida.hefquin.engine.queryproc.QueryCompilationException;
import se.liu.ida.hefquin.engine.queryproc.QueryProcContext;
import se.liu.ida.hefquin.engine.queryproc.SamplingQueryPlanCompiler;

import java.util.ArrayList;
import java.util.List;

public class IteratorBasedSamplingQueryPlanCompilerImpl extends QueryPlanCompilerBase implements SamplingQueryPlanCompiler
{
	public IteratorBasedSamplingQueryPlanCompilerImpl(final QueryProcContext ctxt) {
		super(ctxt);
	}

	@Override
	public ExecutablePlan compile( final PhysicalPlan qep )
			throws QueryCompilationException
	{
		throw new QueryCompilationException("Can't compile query plan without a budget");
	}

	@Override
	public ExecutablePlan compile( final PhysicalPlan qep, final int numberOfWalks )
			throws QueryCompilationException
	{
		final ExecutionContext execCxt = createExecContext();
		final List<SamplingResultElementIterWithNullaryExecOp> leaves = new ArrayList<>();
		final ResultElementIterator it = compile( qep, execCxt, leaves);
		return new IteratorBasedExecutableSamplingPlanImpl( it, numberOfWalks, leaves );
	}

	protected ResultElementIterator compile( final PhysicalPlan qep,
	                                         final ExecutionContext execCxt,
											 final List<SamplingResultElementIterWithNullaryExecOp> leaves)
	{
		if ( qep.numberOfSubPlans() == 0 )
		{
			final NullaryExecutableOp execOp = (NullaryExecutableOp) qep.getRootOperator().createExecOp(true);
			SamplingResultElementIterWithNullaryExecOp sreiwneo =  new SamplingResultElementIterWithNullaryExecOp(execOp, execCxt);
			leaves.add(sreiwneo);
			return sreiwneo;
		}
		else if ( qep.numberOfSubPlans() == 1 )
		{
			final PhysicalPlan subPlan = qep.getSubPlan(0);

			final UnaryExecutableOp execOp = (UnaryExecutableOp) qep.getRootOperator().createExecOp( true, subPlan.getExpectedVariables() );

			final ResultElementIterator elmtIterSubPlan = compile(subPlan, execCxt, leaves);
			return new SamplingResultElementIterWithUnaryExecOp(execOp, elmtIterSubPlan, execCxt);
		}
		else if ( qep.numberOfSubPlans() == 2 )
		{
			final PhysicalPlan subPlan1 = qep.getSubPlan(0);
			final PhysicalPlan subPlan2 = qep.getSubPlan(1);

			final BinaryExecutableOp execOp = (BinaryExecutableOp) qep.getRootOperator().createExecOp(
					true,
					subPlan1.getExpectedVariables(),
					subPlan2.getExpectedVariables() );

			final ResultElementIterator elmtIterSubPlan1 = compile(subPlan1, execCxt, leaves);
			final ResultElementIterator elmtIterSubPlan2 = compile(subPlan2, execCxt, leaves);

			return new SamplingResultElementIterWithBinaryExecOp(execOp, elmtIterSubPlan1, elmtIterSubPlan2, execCxt);
		}
		else {
			List<ResultElementIterator> elementIterators = new ArrayList<>();
			ExpectedVariables[] expectedVariables = new ExpectedVariables[qep.numberOfSubPlans()];

			for (int i = 0; i < qep.numberOfSubPlans(); ++i) {
				final PhysicalPlan subPlan = qep.getSubPlan(i);

				expectedVariables[i] = (subPlan.getExpectedVariables());
			}

			final NaryExecutableOp execOp = (NaryExecutableOp) qep.getRootOperator().createExecOp(
					true,
					expectedVariables);

			for (int i = 0; i < qep.numberOfSubPlans(); ++i) {
				final PhysicalPlan subPlan = qep.getSubPlan(i);

				final ResultElementIterator elmtIterSubPlan = compile(subPlan, execCxt, leaves);
				elementIterators.add(elmtIterSubPlan);
			}

			return new SamplingResultElementIterWithNaryExecOp( execOp, elementIterators, execCxt );
		}
	}

}
