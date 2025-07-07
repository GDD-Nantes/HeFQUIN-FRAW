package se.liu.ida.hefquin.engine;

import org.apache.jena.query.*;
import org.apache.jena.sparql.algebra.optimize.Optimize;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.resultset.ResultsFormat;
import org.apache.jena.sparql.util.QueryExecUtils;
import se.liu.ida.hefquin.base.utils.Pair;
import se.liu.ida.hefquin.engine.federation.access.FederationAccessManager;
import se.liu.ida.hefquin.engine.federation.access.FederationAccessStats;
import se.liu.ida.hefquin.engine.queryproc.QueryProcStats;
import se.liu.ida.hefquin.engine.queryproc.QueryProcessor;
import se.liu.ida.hefquin.jenaintegration.sparql.FrawConstants;
import se.liu.ida.hefquin.jenaintegration.sparql.HeFQUINConstants;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HeFQUINEngineImpl implements HeFQUINEngine
{
	protected final FederationAccessManager fedAccessMgr;
	protected final QueryProcessor qProc;

	public HeFQUINEngineImpl( final FederationAccessManager fedAccessMgr,
	                          final QueryProcessor qProc ) {
		assert fedAccessMgr != null;
		assert qProc != null;

		this.fedAccessMgr = fedAccessMgr;
		this.qProc = qProc;

		Optimize.noOptimizer();
	}

	@Override
	public void integrateIntoJena() {
		ARQ.getContext().setIfUndef(FrawConstants.ENGINE_TO_QPROC, new HashMap<>());
		Map<HeFQUINEngine, QueryProcessor> engineToQProc = ARQ.getContext().get(FrawConstants.ENGINE_TO_QPROC);

		// No need to sync here because integrateIntoJena calls are never made in parallel, it's only called once
		// sequentially for each service during server initialization.
		engineToQProc.put(this, qProc);

		QC.setFactory( ARQ.getContext(), FrawConstants.factory );
	}

	@Override
	public FederationAccessStats getFederationAccessStats() {
		return fedAccessMgr.getStats();
	}

	@Override
	public Pair<QueryProcStats, List<Exception>> executeQuery( final Query query,
	                                                           final ResultsFormat outputFormat,
	                                                           final PrintStream output )
			throws UnsupportedQueryException, IllegalQueryException
	{
		ValuesServiceQueryResolver.expandValuesPlusServicePattern(query);

		final DatasetGraph dsg = DatasetGraphFactory.createGeneral();
		final QueryExecution qe = QueryExecutionFactory.create(query, dsg);

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

	protected void executeSelectQuery( final QueryExecution qe,
	                                   final ResultsFormat outputFormat,
	                                   final PrintStream output ) throws Exception {
		final ResultSet rs;
		try {
			// Every time i run a query, is qe different ...? if it is, then this works, otherwise not a good idea
			qe.getContext().set(FrawConstants.ENGINE, this);
			rs = qe.execSelect();
		}
		catch ( final Exception e ) {
			throw new Exception("Exception occurred when executing a SELECT query using the Jena machinery.", e);
		}

		try {
			QueryExecUtils.outputResultSet(rs, qe.getQuery().getPrologue(), outputFormat, output);
		}
		catch ( final Exception e ) {
			throw new Exception("Exception occurred when outputting the result of a SELECT query using the Jena machinery.", e);
		}
	}

	protected void executeNonSelectQuery( final QueryExecution qe,
	                                      final ResultsFormat outputFormat,
	                                      final PrintStream output ) throws Exception {
		try {
			QueryExecUtils.executeQuery( qe.getQuery().getPrologue(), qe, outputFormat, output );
		}
		catch ( final Exception e ) {
			throw new Exception("Exception occurred when executing an ASK/DESCRIBE/CONSTRUCT query using the Jena machinery.", e);
		}
	}

	@Override
	public void shutdown() {
		fedAccessMgr.shutdown();
		qProc.shutdown();
	}
}
