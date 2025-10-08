package se.liu.ida.hefquin.engine.queryproc;

import se.liu.ida.hefquin.engine.queryplan.executable.ExecutablePlan;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalPlan;
import se.liu.ida.hefquin.base.data.utils.Budget;

public interface SamplingQueryPlanCompiler extends QueryPlanCompiler
{
	ExecutablePlan compile( PhysicalPlan qep, Budget budget ) throws QueryCompilationException;
}
