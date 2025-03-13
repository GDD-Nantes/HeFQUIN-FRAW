package se.liu.ida.hefquin.engine.queryplan.physical.impl;

import se.liu.ida.hefquin.base.query.SPARQLGraphPattern;
import se.liu.ida.hefquin.engine.federation.FederationMember;
import se.liu.ida.hefquin.engine.federation.SPARQLEndpoint;
import se.liu.ida.hefquin.engine.queryplan.executable.UnaryExecutableOp;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ops.ExecOpBindJoinSPARQLwithVALUES;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.*;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalPlanVisitor;

/**
 * A physical operator that implements (a batching version of) the bind
 * join algorithm using a VALUES clause to capture the potential join
 * partners that are sent to the federation member.
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
 * to {@link ExecOpBindJoinSPARQLwithVALUES}, which provides the
 * implementation of this algorithm.
 * </p>
 */
public class PhysicalOpFrawBindJoinWithVALUES extends PhysicalOpBindJoinWithVALUES
{
	public PhysicalOpFrawBindJoinWithVALUES(final LogicalOpTPAdd lop ) {
		super(lop);
		assert lop.getFederationMember() instanceof SPARQLEndpoint;
	}

	public PhysicalOpFrawBindJoinWithVALUES(final LogicalOpTPOptAdd lop ) {
		super(lop);
		assert lop.getFederationMember() instanceof SPARQLEndpoint;
	}

	public PhysicalOpFrawBindJoinWithVALUES(final LogicalOpBGPAdd lop ) {
		super(lop);
		assert lop.getFederationMember() instanceof SPARQLEndpoint;
	}

	public PhysicalOpFrawBindJoinWithVALUES(final LogicalOpBGPOptAdd lop ) {
		super(lop);
		assert lop.getFederationMember() instanceof SPARQLEndpoint;
	}

	public PhysicalOpFrawBindJoinWithVALUES(final LogicalOpGPAdd lop ) {
		super(lop);
		assert lop.getFederationMember() instanceof SPARQLEndpoint;
	}

	public PhysicalOpFrawBindJoinWithVALUES(final LogicalOpGPOptAdd lop ) {
		super(lop);
		assert lop.getFederationMember() instanceof SPARQLEndpoint;
	}

	@Override
	public boolean equals( final Object o ) {
		return o instanceof PhysicalOpFrawBindJoinWithVALUES
				&& ((PhysicalOpFrawBindJoinWithVALUES) o).lop.equals(lop);
	}

	protected UnaryExecutableOp createExecOp( final SPARQLGraphPattern pattern,
	                                          final FederationMember fm,
	                                          final boolean useOuterJoinSemantics,
	                                          final boolean collectExceptions ) {
		if ( fm instanceof SPARQLEndpoint )
			return new ExecOpBindJoinSPARQLwithVALUES( pattern, (SPARQLEndpoint) fm, useOuterJoinSemantics, collectExceptions );
		else
			throw new IllegalArgumentException("Unsupported type of federation member: " + fm.getClass().getName() );
	}

	@Override
	public void visit(final PhysicalPlanVisitor visitor) {
		visitor.visit(this);
	}

	@Override
	public String toString() {

		return "> VALUESFrawBindJoin" + lop.toString();
	}

}
