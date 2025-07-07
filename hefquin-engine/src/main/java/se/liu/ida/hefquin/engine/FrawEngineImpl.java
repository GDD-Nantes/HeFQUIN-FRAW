package se.liu.ida.hefquin.engine;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.riot.rowset.RowSetWriterRegistry;
import org.apache.jena.sparql.algebra.optimize.Optimize;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.expr.aggregate.AggregateRegistry;
import org.apache.jena.sparql.resultset.ResultsFormat;
import org.apache.jena.sparql.util.QueryExecUtils;
import se.liu.ida.hefquin.base.utils.Pair;
import se.liu.ida.hefquin.engine.federation.access.FederationAccessManager;
import se.liu.ida.hefquin.engine.queryproc.QueryProcStats;
import se.liu.ida.hefquin.engine.queryproc.QueryProcessor;
import se.liu.ida.hefquin.jenaintegration.sparql.FrawConstants;
import se.liu.ida.hefquin.jenaintegration.sparql.HeFQUINConstants;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

public class FrawEngineImpl extends HeFQUINEngineImpl implements FrawEngine
{

	protected final int budget;
	protected final int subBudget;

	public FrawEngineImpl(final FederationAccessManager fedAccessMgr,
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

	public Pair<QueryProcStats, List<Exception>> executeQuery(Query query, ResultsFormat outputFormat, PrintStream output, Integer budget, Integer subBudget)
			throws UnsupportedQueryException, IllegalQueryException{


		ValuesServiceQueryResolver.expandValuesPlusServicePattern(query);

		final DatasetGraph dsg = DatasetGraphFactory.createGeneral();
		final QueryExecution qe = QueryExecutionFactory.create(query, dsg);

		qe.getContext().set(FrawConstants.BUDGET, budget);
		qe.getContext().set(FrawConstants.SUB_BUDGET, subBudget);

		Exception ex = null;
		try {
			if ( query.isSelectType() )
				executeSelectQuery(qe, outputFormat, output);
			else
				QueryExecUtils.executeQuery(query.getPrologue(), qe, outputFormat, output);
		}
		catch ( final Exception e ) {
			ex = e;
		}

		final QueryProcStats stats = (QueryProcStats) qe.getContext().get(HeFQUINConstants.sysQueryProcStats);


		@SuppressWarnings("unchecked")
		List<Exception> exceptions = (List<Exception>) qe.getContext().get(HeFQUINConstants.sysQueryProcExceptions);
		if ( ex != null ) {
			if ( exceptions == null ) {
				exceptions = new ArrayList<>();
			}
			exceptions.add(ex);
		}

		return new Pair<>(stats, exceptions);
	}

	public Integer getBudget() {
		return budget;
	}

	public Integer getSubBudget() {
		return subBudget;
	}

}
