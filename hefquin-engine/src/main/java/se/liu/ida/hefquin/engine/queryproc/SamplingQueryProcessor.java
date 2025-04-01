package se.liu.ida.hefquin.engine.queryproc;

import se.liu.ida.hefquin.base.query.Query;
import se.liu.ida.hefquin.base.utils.Pair;

import java.util.List;

public interface SamplingQueryProcessor extends QueryProcessor
{
	Pair<QueryProcStats, List<Exception>> processQuery( final Query query, final QueryResultSink resultSink, int numberOfRandomWalks )
			throws QueryProcException;
}
