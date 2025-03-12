package se.liu.ida.hefquin.engine.queryproc;

import se.liu.ida.hefquin.base.query.Query;
import se.liu.ida.hefquin.base.utils.Pair;

import java.util.List;

public interface QueryProcessor
{
	Pair<QueryProcStats, List<Exception>> processQuery( final Query query, final QueryResultSink resultSink )
			throws QueryProcException, NoQueryToProcessException;

	QueryPlanner getPlanner();
	QueryPlanCompiler getPlanCompiler();
	ExecutionEngine getExecutionEngine();
}
