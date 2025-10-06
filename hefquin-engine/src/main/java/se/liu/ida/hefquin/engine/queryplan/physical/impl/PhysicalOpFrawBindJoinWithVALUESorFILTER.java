package se.liu.ida.hefquin.engine.queryplan.physical.impl;

import se.liu.ida.hefquin.base.query.ExpectedVariables;
import se.liu.ida.hefquin.base.query.SPARQLGraphPattern;
import se.liu.ida.hefquin.engine.queryplan.executable.UnaryExecutableOp;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ops.ExecOpBindJoinSPARQLwithVALUESorFILTER;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ops.ExecOpFrawBindJoinSPARQLwithVALUESorFILTER;
import se.liu.ida.hefquin.engine.queryplan.info.QueryPlanningInfo;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalOpGPAdd;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalOpGPOptAdd;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalPlanVisitor;
import se.liu.ida.hefquin.federation.FederationMember;
import se.liu.ida.hefquin.federation.SPARQLEndpoint;

/**
 * A physical operator that implements (a batching version of) the bind
 * join algorithm. It starts by using a VALUES clause to capture the potential join
 * partners that are sent to the federation member. If this fails, it uses the bind join
 * algorithm with a FILTER clause instead.

 * <p>
 * <b>Algorithm description:</b> For a detailed description of the
 * actual algorithm associated with this physical operator, refer
 * to {@link ExecOpBindJoinSPARQLwithVALUESorFILTER}, which provides the
 * implementation of this algorithm.
 * </p>
 */
public class PhysicalOpFrawBindJoinWithVALUESorFILTER extends PhysicalOpBindJoinWithVALUESorFILTER
{
	public PhysicalOpFrawBindJoinWithVALUESorFILTER( final LogicalOpGPAdd lop ) {
		super(lop);
		assert lop.getFederationMember() instanceof SPARQLEndpoint;
	}

	public PhysicalOpFrawBindJoinWithVALUESorFILTER( final LogicalOpGPOptAdd lop ) {
		super(lop);
		assert lop.getFederationMember() instanceof SPARQLEndpoint;
	}

	@Override
	public boolean equals( final Object o ) {
		return o instanceof PhysicalOpFrawBindJoinWithVALUESorFILTER
				&& ((PhysicalOpFrawBindJoinWithVALUESorFILTER) o).lop.equals(lop);
	}

	@Override
	public UnaryExecutableOp createExecOp( final SPARQLGraphPattern pattern,
										   final SPARQLEndpoint sparqlEndpoint,
										   final boolean useOuterJoinSemantics,
										   final boolean collectExceptions,
										   final QueryPlanningInfo qpInfo,
										   final ExpectedVariables... inputVars ) {
		return new ExecOpFrawBindJoinSPARQLwithVALUESorFILTER( pattern,
				sparqlEndpoint,
				inputVars[0],
				useOuterJoinSemantics,
				ExecOpBindJoinSPARQLwithVALUESorFILTER.DEFAULT_BATCH_SIZE,
				collectExceptions,
				qpInfo );
	}

	@Override
	public void visit(final PhysicalPlanVisitor visitor) {
		visitor.visit(this);
	}

	@Override
	public String toString() {
		return "> VALUESorFILTERFrawBindJoin" + lop.toString();
	}

}
