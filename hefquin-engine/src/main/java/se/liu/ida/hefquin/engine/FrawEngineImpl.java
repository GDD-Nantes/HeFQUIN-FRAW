package se.liu.ida.hefquin.engine;

import org.apache.jena.query.ARQ;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.riot.rowset.RowSetWriterRegistry;
import org.apache.jena.sparql.algebra.optimize.Optimize;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.expr.aggregate.AggregateRegistry;
import se.liu.ida.hefquin.engine.federation.access.FederationAccessManager;
import se.liu.ida.hefquin.engine.queryproc.QueryProcessor;
import se.liu.ida.hefquin.engine.queryproc.SamplingQueryProcessor;
import se.liu.ida.hefquin.jenaintegration.sparql.engine.main.OpExecutorFraw;

public class FrawEngineImpl extends HeFQUINEngineImpl
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

		AggregateRegistry.register("http://customAgg/rawcount", RawCountAggregator.factory());
		AggregateRegistry.register("http://customAgg/rawaverage", RawAverageAggregator.factory());

		RowSetWriterRegistry.register(ResultSetLang.RS_JSON, RawRowSetWriterJSON.factory);

		Optimize.noOptimizer();
	}

	@Override
	public void integrateIntoJena() {
		OpExecutorFactory factory = execCxt -> new OpExecutorFraw((SamplingQueryProcessor) qProc, execCxt, budget, subBudget);

		QC.setFactory( ARQ.getContext(), factory );
	}


}
