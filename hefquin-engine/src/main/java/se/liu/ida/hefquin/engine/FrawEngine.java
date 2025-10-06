package se.liu.ida.hefquin.engine;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.riot.rowset.RowSetWriterRegistry;
import org.apache.jena.sparql.algebra.optimize.Optimize;
import org.apache.jena.sparql.expr.aggregate.AggregateRegistry;
import org.apache.jena.sparql.resultset.ResultsFormat;
import se.liu.ida.hefquin.engine.queryproc.QueryProcessor;
import se.liu.ida.hefquin.engine.queryproc.impl.QueryProcessingStatsAndExceptionsImpl;
import se.liu.ida.hefquin.federation.access.FederationAccessManager;
import se.liu.ida.hefquin.jenaintegration.sparql.FrawConstants;
import se.liu.ida.hefquin.jenaintegration.sparql.HeFQUINConstants;

import java.io.PrintStream;
import java.util.Arrays;

public class FrawEngine extends HeFQUINEngine
{

	protected final int budget;
	protected final int subBudget;

	public FrawEngine(final FederationAccessManager fedAccessMgr,
					  final QueryProcessor qProc,
					  final int budget,
					  final int subBudget) {
		super( fedAccessMgr, qProc );

		assert budget >= 0;
		assert subBudget >= 0;

		this.budget = budget;
		this.subBudget = subBudget;

		initialize();
	}

	protected void initialize() {
		AggregateRegistry.register("http://customAgg/rawcount", RawCountAggregator.factory());
		AggregateRegistry.register("http://customAgg/rawaverage", RawAverageAggregator.factory());

		RowSetWriterRegistry.register(ResultSetLang.RS_JSON, RawRowSetWriterJSON.factory);

		Optimize.noOptimizer();
	}

	protected QueryExecution _prepareExecution( final Query query )
			throws UnsupportedQueryException, IllegalQueryException
	{
		QueryExecution qe = super._prepareExecution(query);

		qe.getContext().set(FrawConstants.ENGINE, this);


		return qe;
	}

	public QueryProcessingStatsAndExceptions executeQuery(Query query, ResultsFormat outputFormat, PrintStream output, Integer budget, Integer subBudget)
			throws UnsupportedQueryException, IllegalQueryException{

		QueryExecution qe = _prepareExecution(query);

		qe.getContext().set(FrawConstants.ENGINE, this);
		qe.getContext().set(FrawConstants.BUDGET, budget);
		qe.getContext().set(FrawConstants.SUB_BUDGET, subBudget);

		Exception ex = null;
		try {
			if ( query.isSelectType() )
				_execSelectQuery(qe, outputFormat, output);
			else
				_execNonSelectQuery(qe, outputFormat, output);
		}
		catch ( final Exception e ) {
			ex = e;
		}

		final QueryProcessingStatsAndExceptions stats = qe.getContext().get(HeFQUINConstants.sysQProcStatsAndExceptions);

		if ( ex == null ) {
			return stats;
		}
		else if ( stats == null ) {
			return new QueryProcessingStatsAndExceptionsImpl( -1L, -1L, -1L, -1L, null, null, Arrays.asList(ex) );
		}
		else {
			return new QueryProcessingStatsAndExceptionsImpl(stats, ex);
		}
	}

	public Integer getBudget() {
		return budget;
	}

	public Integer getSubBudget() {
		return subBudget;
	}

}
