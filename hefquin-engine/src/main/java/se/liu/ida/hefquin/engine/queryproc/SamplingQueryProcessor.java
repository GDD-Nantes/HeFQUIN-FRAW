package se.liu.ida.hefquin.engine.queryproc;

import se.liu.ida.hefquin.base.query.Query;
import se.liu.ida.hefquin.engine.QueryProcessingStatsAndExceptions;

public interface SamplingQueryProcessor extends QueryProcessor
{
	QueryProcessingStatsAndExceptions processQuery(final Query query, final QueryResultSink resultSink, int numberOfRandomWalks )
			throws QueryProcException;
}
