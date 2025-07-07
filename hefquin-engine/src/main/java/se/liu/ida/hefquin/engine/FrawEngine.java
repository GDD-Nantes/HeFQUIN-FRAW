package se.liu.ida.hefquin.engine;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.resultset.ResultsFormat;
import se.liu.ida.hefquin.base.utils.Pair;
import se.liu.ida.hefquin.engine.queryproc.QueryProcStats;

import java.io.PrintStream;
import java.util.List;

public interface FrawEngine extends HeFQUINEngine
{
	Pair<QueryProcStats, List<Exception>> executeQuery( Query query,
														ResultsFormat outputFormat,
														PrintStream output,
														Integer budget,
														Integer subBudget)
			throws UnsupportedQueryException, IllegalQueryException;

	Integer getBudget();

	Integer getSubBudget();
}
