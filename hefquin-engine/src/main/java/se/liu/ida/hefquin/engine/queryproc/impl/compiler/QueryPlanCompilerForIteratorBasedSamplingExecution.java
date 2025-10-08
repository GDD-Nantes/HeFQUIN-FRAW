package se.liu.ida.hefquin.engine.queryproc.impl.compiler;

import se.liu.ida.hefquin.base.query.ExpectedVariables;
import se.liu.ida.hefquin.engine.queryplan.executable.*;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.iterbased.*;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ops.BaseForExecOpFrawBindJoinSPARQL;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ops.ExecOpFrawRequest;
import se.liu.ida.hefquin.engine.queryplan.info.QueryPlanningInfo;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalPlan;
import se.liu.ida.hefquin.engine.queryproc.ExecutionContext;
import se.liu.ida.hefquin.engine.queryproc.QueryCompilationException;
import se.liu.ida.hefquin.engine.queryproc.QueryProcContext;
import se.liu.ida.hefquin.engine.queryproc.SamplingQueryPlanCompiler;
import se.liu.ida.hefquin.base.data.utils.Budget;

import java.util.ArrayList;
import java.util.List;

public class QueryPlanCompilerForIteratorBasedSamplingExecution extends QueryPlanCompilerBase implements SamplingQueryPlanCompiler
{
	public QueryPlanCompilerForIteratorBasedSamplingExecution(final QueryProcContext ctxt) {
		super(ctxt);
	}

	@Override
	public ExecutablePlan compile( final PhysicalPlan qep )
			throws QueryCompilationException
	{
		throw new QueryCompilationException("Can't compile query plan without a budget");
	}

	@Override
	public ExecutablePlan compile( final PhysicalPlan qep, final Budget budget )
			throws QueryCompilationException
	{
		Budget remoteBudget = new Budget()
				.setAttempts(budget.getRemoteAttempts());
		
		final ExecutionContext execCxt = createExecContext();
		final List<SamplingResultElementIterWithNullaryExecOp> leaves = new ArrayList<>();
		final List<SamplingResultElementIterWithUnaryExecOp> unaries = new ArrayList<>();
		
		final ResultElementIterator it = compile( qep, execCxt, leaves, unaries);
		

		// Setting budget for fraw request, or operators that create fraw requests
		leaves.stream()
				.map(SamplingResultElementIterWithNullaryExecOp::getOp)
				.filter(e -> e instanceof ExecOpFrawRequest)
				.map(e -> (ExecOpFrawRequest) e)
				.forEach(e -> e.setBudget(remoteBudget));
		
		unaries.stream()
				.map(SamplingResultElementIterWithUnaryExecOp::getOp)
				.filter(e -> e instanceof BaseForExecOpFrawBindJoinSPARQL)
				.map(e -> (BaseForExecOpFrawBindJoinSPARQL) e)
				.forEach(e -> e.setRequestBudget(remoteBudget));
		
		
		return new IteratorBasedExecutableSamplingPlanImpl( it, budget, leaves );
	}

	protected ResultElementIterator compile( final PhysicalPlan qep,
	                                         final ExecutionContext execCxt,
											 final List<SamplingResultElementIterWithNullaryExecOp> leaves,
											 final List<SamplingResultElementIterWithUnaryExecOp> unaries )
	{
		final QueryPlanningInfo qpInfo;
		if ( qep.hasQueryPlanningInfo() )
			qpInfo = qep.getQueryPlanningInfo();
		else
			qpInfo = null;

		if ( qep.numberOfSubPlans() == 0 )
		{
			final NullaryExecutableOp execOp = (NullaryExecutableOp) qep.getRootOperator().createExecOp(true, qpInfo);
			SamplingResultElementIterWithNullaryExecOp sreiwneo = new SamplingResultElementIterWithNullaryExecOp(execOp, execCxt);
			leaves.add(sreiwneo);
			return sreiwneo;
		}
		else if ( qep.numberOfSubPlans() == 1 )
		{
			final PhysicalPlan subPlan = qep.getSubPlan(0);

			final UnaryExecutableOp execOp = (UnaryExecutableOp) qep.getRootOperator().createExecOp( true, qpInfo, subPlan.getExpectedVariables() );

			final ResultElementIterator elmtIterSubPlan = compile(subPlan, execCxt, leaves, unaries);
			SamplingResultElementIterWithUnaryExecOp sreiwueo = new SamplingResultElementIterWithUnaryExecOp(execOp, elmtIterSubPlan, execCxt);
			unaries.add(sreiwueo);
			return sreiwueo;
		}
		else if ( qep.numberOfSubPlans() == 2 )
		{
			final PhysicalPlan subPlan1 = qep.getSubPlan(0);
			final PhysicalPlan subPlan2 = qep.getSubPlan(1);

			final BinaryExecutableOp execOp = (BinaryExecutableOp) qep.getRootOperator().createExecOp(
					true,
					qpInfo,
					subPlan1.getExpectedVariables(),
					subPlan2.getExpectedVariables() );

			final ResultElementIterator elmtIterSubPlan1 = compile(subPlan1, execCxt, leaves, unaries);
			final ResultElementIterator elmtIterSubPlan2 = compile(subPlan2, execCxt, leaves, unaries);

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
					qpInfo,
					expectedVariables);

			for (int i = 0; i < qep.numberOfSubPlans(); ++i) {
				final PhysicalPlan subPlan = qep.getSubPlan(i);

				final ResultElementIterator elmtIterSubPlan = compile(subPlan, execCxt, leaves, unaries);
				elementIterators.add(elmtIterSubPlan);
			}

			return new SamplingResultElementIterWithNaryExecOp( execOp, elementIterators, execCxt );
		}
	}

}
