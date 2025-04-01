package se.liu.ida.hefquin.engine.queryproc;

import se.liu.ida.hefquin.engine.queryplan.executable.ExecutablePlan;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalPlan;

public interface SamplingQueryPlanCompiler extends QueryPlanCompiler
{
	ExecutablePlan compile( PhysicalPlan qep, int numberOfRandomWalks ) throws QueryCompilationException;
}
