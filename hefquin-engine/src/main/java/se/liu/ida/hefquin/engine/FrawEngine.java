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
import se.liu.ida.hefquin.base.data.utils.Budget;
import se.liu.ida.hefquin.jenaintegration.sparql.HeFQUINConstants;

import java.io.PrintStream;
import java.util.Arrays;

import static se.liu.ida.hefquin.jenaintegration.sparql.FrawConstants.*;

public class FrawEngine extends HeFQUINEngine
{
	protected final Budget defaultBudget;
	protected final Budget maxBudget;

	public FrawEngine(final FederationAccessManager fedAccessMgr,
					  final QueryProcessor qProc,
					  final Budget defaultBudget,
					  final Budget maxBudget) {
		super( fedAccessMgr, qProc );

		this.defaultBudget = defaultBudget;
		this.maxBudget = maxBudget;

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

		qe.getContext().set(ENGINE, this);


		return qe;
	}

	public QueryProcessingStatsAndExceptions executeQuery(Query query, ResultsFormat outputFormat, PrintStream output, Budget requestBudget)
			throws UnsupportedQueryException, IllegalQueryException{

		QueryExecution qe = _prepareExecution(query);

		qe.getContext().set(ENGINE, this);

		requestBudget.fillWith(defaultBudget);
		Budget queryExecutionBudget = Budget.mergeMin(requestBudget, maxBudget);

		qe.getContext().set(QUERY_EXECUTION_BUDGET, queryExecutionBudget);


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

	public Budget getDefaultBudget(){
		return defaultBudget;
	}

	public Budget getMaxBudget(){
		return maxBudget;
	}
}
