package se.liu.ida.hefquin.engine.queryplan.physical.impl;

import se.liu.ida.hefquin.base.query.SPARQLGraphPattern;
import se.liu.ida.hefquin.base.queryplan.ExpectedVariables;
import se.liu.ida.hefquin.engine.federation.FederationMember;
import se.liu.ida.hefquin.engine.federation.FederationMemberAgglomeration;
import se.liu.ida.hefquin.engine.federation.SPARQLEndpoint;
import se.liu.ida.hefquin.engine.queryplan.executable.UnaryExecutableOp;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ops.ExecOpFrawBindJoin;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ops.ExecOpFrawBindJoinSPARQL;
import se.liu.ida.hefquin.engine.queryplan.logical.UnaryLogicalOp;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.*;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalPlanVisitor;

public class PhysicalOpFrawBindJoin extends BaseForPhysicalOpSingleInputJoin{

    public PhysicalOpFrawBindJoin(UnaryLogicalOp lop) {
        super(lop);
    }

    @Override
    public UnaryExecutableOp createExecOp(boolean collectExceptions, ExpectedVariables... inputVars) {

        assert inputVars.length == 1;
        if ( ! inputVars[0].getPossibleVariables().isEmpty() ) {
            // The executable operator for this physical operator (i.e., ExecOpBindJoinSPARQLwithVALUES)
            // can work correctly only in cases in which all input solution mappings are for the exact
            // same set of variables. This can be guaranteed only if the set of possible variables from
            // the child operator is empty.
            throw new IllegalArgumentException("Nonempty set of possible variables.");
        }

        final SPARQLGraphPattern pt;
        final FederationMember fm;
        final boolean useOuterJoinSemantics;

        if ( lop instanceof LogicalOpTPAdd tpAdd ) {
            pt = tpAdd.getTP();
            fm = tpAdd.getFederationMember();
            useOuterJoinSemantics = false;
        }
        else if ( lop instanceof LogicalOpTPOptAdd tpOptAdd ) {
            pt = tpOptAdd.getTP();
            fm = tpOptAdd.getFederationMember();
            useOuterJoinSemantics = true;
        }
        else if ( lop instanceof LogicalOpBGPAdd bgpAdd ) {
            pt = bgpAdd.getBGP();
            fm = bgpAdd.getFederationMember();
            useOuterJoinSemantics = false;
        }
        else if ( lop instanceof LogicalOpBGPOptAdd bgpOptAdd ) {
            pt = bgpOptAdd.getBGP();
            fm = bgpOptAdd.getFederationMember();
            useOuterJoinSemantics = true;
        }
        else if ( lop instanceof LogicalOpGPAdd gpAdd ) {
            pt = gpAdd.getPattern();
            fm = gpAdd.getFederationMember();
            useOuterJoinSemantics = false;
        }
        else if ( lop instanceof LogicalOpGPOptAdd gpOptAdd ) {
            pt = gpOptAdd.getPattern();
            fm = gpOptAdd.getFederationMember();
            useOuterJoinSemantics = true;
        }
        else {
            throw new IllegalArgumentException("Unsupported type of operator: " + lop.getClass().getName() );
        }

        return createExecOp(pt, fm, useOuterJoinSemantics, collectExceptions);
    }

    protected UnaryExecutableOp createExecOp( final SPARQLGraphPattern pattern,
                                              final FederationMember fm,
                                              final boolean useOuterJoinSemantics,
                                              final boolean collectExceptions ) {
        if ( fm instanceof SPARQLEndpoint )
            return new ExecOpFrawBindJoinSPARQL(pattern, (SPARQLEndpoint) fm, useOuterJoinSemantics, collectExceptions );
        else if ( fm instanceof FederationMemberAgglomeration )
            return new ExecOpFrawBindJoin(pattern, (FederationMemberAgglomeration) fm, useOuterJoinSemantics, collectExceptions );
        else
            throw new IllegalArgumentException("Unsupported type of federation member: " + fm.getClass().getName() );
    }

    @Override
    public void visit(PhysicalPlanVisitor visitor) {
        visitor.visit(this);
    }

}
