package se.liu.ida.hefquin.engine.queryproc;

import se.liu.ida.hefquin.base.query.Query;
import se.liu.ida.hefquin.engine.QueryProcessingStatsAndExceptions;
import se.liu.ida.hefquin.base.data.utils.Budget;

public interface SamplingQueryProcessor extends QueryProcessor
{
	QueryProcessingStatsAndExceptions processQuery(final Query query, final QueryResultSink resultSink, Budget budget)
			throws QueryProcException;
}
