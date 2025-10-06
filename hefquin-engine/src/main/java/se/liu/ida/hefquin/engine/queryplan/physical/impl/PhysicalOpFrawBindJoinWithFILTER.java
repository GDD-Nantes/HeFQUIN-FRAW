package se.liu.ida.hefquin.engine.queryplan.physical.impl;

import se.liu.ida.hefquin.base.query.ExpectedVariables;
import se.liu.ida.hefquin.base.query.SPARQLGraphPattern;
import se.liu.ida.hefquin.engine.queryplan.executable.UnaryExecutableOp;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ops.ExecOpBindJoinSPARQLwithFILTER;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ops.ExecOpFrawBindJoinSPARQLwithFILTER;
import se.liu.ida.hefquin.engine.queryplan.info.QueryPlanningInfo;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalOpGPAdd;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalOpGPOptAdd;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalPlanVisitor;
import se.liu.ida.hefquin.federation.FederationMember;
import se.liu.ida.hefquin.federation.SPARQLEndpoint;

/**
 * A physical operator that implements (a batching version of) the bind
 * join algorithm using FILTERs to capture the potential join partners
 * that are sent to the federation member.
 *
 * <p>
 * <b>Semantics:</b> This operator implements the logical operators gpAdd
 * (see {@link LogicalOpGPAdd}) and gpOptAdd (see {@link LogicalOpGPOptAdd}).
 * That is, for a given graph pattern, a federation  member, and an input
 * sequence of solution mappings (produced by the sub-plan under this
 * operator), the operator produces the solutions resulting from the join
 * (inner or left outer) between the input solutions and the solutions of
 * evaluating the given graph pattern over the data of the federation
 * member.
 * </p>
 *
 * <p>
 * <b>Algorithm description:</b> For a detailed description of the
 * actual algorithm associated with this physical operator, refer
 * to {@link ExecOpBindJoinSPARQLwithFILTER}, which provides the
 * implementation of this algorithm.
 * </p>
 */
public class PhysicalOpFrawBindJoinWithFILTER extends PhysicalOpBindJoinWithFILTER
{
	public PhysicalOpFrawBindJoinWithFILTER(final LogicalOpGPAdd lop ) {
		super(lop);

		assert lop.getFederationMember() instanceof SPARQLEndpoint;
	}

	public PhysicalOpFrawBindJoinWithFILTER(final LogicalOpGPOptAdd lop ) {
		super(lop);

		assert lop.getFederationMember() instanceof SPARQLEndpoint;
	}

	@Override
	public boolean equals( final Object o ) {
		return o instanceof PhysicalOpFrawBindJoinWithFILTER
				&& ((PhysicalOpFrawBindJoinWithFILTER) o).lop.equals(lop);
	}

	@Override
	public UnaryExecutableOp createExecOp( final SPARQLGraphPattern pattern,
										   final SPARQLEndpoint sparqlEndpoint,
										   final boolean useOuterJoinSemantics,
										   final boolean collectExceptions,
										   final QueryPlanningInfo qpInfo,
										   final ExpectedVariables... inputVars ) {
		return new ExecOpFrawBindJoinSPARQLwithFILTER( pattern,
				sparqlEndpoint,
				inputVars[0],
				useOuterJoinSemantics,
				ExecOpBindJoinSPARQLwithFILTER.DEFAULT_BATCH_SIZE,
				collectExceptions,
				qpInfo );
	}

//	@Override
//	public UnaryExecutableOp createExecOp( final boolean collectExceptions,
//										   final ExpectedVariables... inputVars ) {
//		final SPARQLGraphPattern pt;
//		final FederationMember fm;
//		final boolean useOuterJoinSemantics;
//
//		if ( lop instanceof LogicalOpGPAdd gpAdd ) {
//			pt = gpAdd.getPattern();
//			fm = gpAdd.getFederationMember();
//			useOuterJoinSemantics = false;
//		}
//		else if ( lop instanceof LogicalOpGPOptAdd gpOptAdd ) {
//			pt = gpOptAdd.getPattern();
//			fm = gpOptAdd.getFederationMember();
//			useOuterJoinSemantics = true;
//		}
//		else {
//			throw new IllegalArgumentException("Unsupported type of operator: " + lop.getClass().getName() );
//		}
//
//		if ( fm instanceof SPARQLEndpoint sparqlEndpoint )
//			return new ExecOpFrawBindJoinSPARQLwithFILTER( pt,
//					sparqlEndpoint,
//					inputVars[0],
//					useOuterJoinSemantics,
//					ExecOpBindJoinSPARQLwithFILTER.DEFAULT_BATCH_SIZE,
//					collectExceptions,
//					qpInfo );
//		else
//			throw new IllegalArgumentException("Unsupported type of federation member: " + fm.getClass().getName() );
//	}

	@Override
	public void visit(final PhysicalPlanVisitor visitor) {
		visitor.visit(this);
	}

	@Override
	public String toString() {

		return "> FILTERFrawBindJoin " + "(" + getID() + ") " +  lop.toString();
	}

}
