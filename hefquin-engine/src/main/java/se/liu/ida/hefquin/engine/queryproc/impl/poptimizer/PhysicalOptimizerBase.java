package se.liu.ida.hefquin.engine.queryproc.impl.poptimizer;

import se.liu.ida.hefquin.base.utils.Pair;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlan;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalPlan;
import se.liu.ida.hefquin.engine.queryplan.utils.LogicalToPhysicalPlanConverter;
import se.liu.ida.hefquin.engine.queryproc.PhysicalOptimizationException;
import se.liu.ida.hefquin.engine.queryproc.PhysicalOptimizationStats;
import se.liu.ida.hefquin.engine.queryproc.PhysicalOptimizer;

public abstract class PhysicalOptimizerBase implements PhysicalOptimizer
{
	protected final LogicalToPhysicalPlanConverter l2pConverter;

	public PhysicalOptimizerBase( final LogicalToPhysicalPlanConverter l2pConverter ) {
		assert l2pConverter != null;
		this.l2pConverter = l2pConverter;
	}

	@Override
	public final Pair<PhysicalPlan, PhysicalOptimizationStats> optimize( final LogicalPlan lp )
			throws PhysicalOptimizationException {
		final boolean keepMultiwayJoins = keepMultiwayJoinsInInitialPhysicalPlan();
		final PhysicalPlan initialPhysicalPlan = l2pConverter.convert(lp, keepMultiwayJoins);
		return optimize(initialPhysicalPlan);
	}

	/**
	 * Return true if this optimizer expects that multiway joins are carried
	 * over from the given logical plan into the initial physical plan. The
	 * {@link #optimize(LogicalPlan)} function passes this flag as the second
	 * argument of the
	 * {@link LogicalToPhysicalPlanConverter#convert(LogicalPlan, boolean)}
	 * function.
	 */
	protected abstract boolean keepMultiwayJoinsInInitialPhysicalPlan();

	protected abstract Pair<PhysicalPlan, PhysicalOptimizationStats> optimize( PhysicalPlan initialPhysicalPlan ) throws PhysicalOptimizationException;
}
