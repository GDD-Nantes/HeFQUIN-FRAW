package se.liu.ida.hefquin.engine.queryplan.executable.impl.ops;

import org.apache.jena.sparql.engine.binding.Binding;
import se.liu.ida.hefquin.base.data.utils.Budget;
import se.liu.ida.hefquin.base.query.ExpectedVariables;
import se.liu.ida.hefquin.base.query.SPARQLGraphPattern;
import se.liu.ida.hefquin.engine.queryplan.executable.NullaryExecutableOp;
import se.liu.ida.hefquin.engine.queryplan.info.QueryPlanningInfo;
import se.liu.ida.hefquin.federation.SPARQLEndpoint;
import se.liu.ida.hefquin.federation.access.impl.req.SPARQLRequestImpl;

import java.util.Set;

/**
 * A base class for all variations of the bind join algorithm that use
 * some form of SPARQL requests.
 */
public abstract class BaseForExecOpFrawBindJoinSPARQL extends BaseForExecOpFrawBindJoinWithRequestOps<SPARQLGraphPattern, SPARQLEndpoint>
{
	protected Budget budget;

	public BaseForExecOpFrawBindJoinSPARQL(final SPARQLGraphPattern p,
										   final SPARQLEndpoint fm,
										   final ExpectedVariables inputVars,
										   final boolean useOuterJoinSemantics,
										   final int batchSize,
										   final boolean collectExceptions,
										   final QueryPlanningInfo qpInfo) {
		super(p, p.getAllMentionedVariables(), fm, inputVars, useOuterJoinSemantics, batchSize, collectExceptions, qpInfo);
	}

	@Override
	protected NullaryExecutableOp createExecutableReqOpForAll() {
		return new ExecOpFrawRequest( new SPARQLRequestImpl(query), fm, false, qpInfo );
	}

	protected abstract NullaryExecutableOp _createExecutableReqOp(Set<Binding> solMaps);

	@Override
	protected NullaryExecutableOp createExecutableReqOp( Set<Binding> solMaps ){
		if(this.budget == null) throw new UnsupportedOperationException("Can't create executable req for null budget");

		return _createExecutableReqOp(solMaps);
	}


	public void setRequestBudget(Budget budget) {
		this.budget = budget;
	}
}
