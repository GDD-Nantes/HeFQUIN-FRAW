package se.liu.ida.hefquin.engine.queryproc.impl.srcsel;

import org.apache.jena.sparql.algebra.Op;
import se.liu.ida.hefquin.base.utils.Pair;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlan;
import se.liu.ida.hefquin.engine.queryproc.QueryProcContext;
import se.liu.ida.hefquin.engine.queryproc.SourcePlanner;
import se.liu.ida.hefquin.engine.queryproc.SourcePlanningException;
import se.liu.ida.hefquin.engine.queryproc.SourcePlanningStats;

/**
 * This implementation of {@link SourcePlanner} does not actually perform
 * query decomposition and source selection but simply assumes queries with
 * SERVICE clauses where, for the moment, all of these SERVICE clauses are
 * of the form "SERVICE uri {...}" (i.e., not "SERVICE var {...}"). Therefore,
 * all that this implementation does is to convert the given query pattern
 * into a logical plan.
 */
public class ServiceClauseBasedJOUSourcePlannerImpl extends ServiceClauseBasedSourcePlannerImpl
{
	public ServiceClauseBasedJOUSourcePlannerImpl(final QueryProcContext ctxt ) {
		super(ctxt);
	}

	@Override
	protected Pair<LogicalPlan, SourcePlanningStats> createSourceAssignment( final Op jenaOp )
			throws SourcePlanningException
	{
		Op jouOp = new JOUConverter().convert( jenaOp );
		return super.createSourceAssignment( jouOp );
	}

}
