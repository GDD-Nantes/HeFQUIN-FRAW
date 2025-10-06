package se.liu.ida.hefquin.engine.queryplan.physical.impl;

import se.liu.ida.hefquin.base.query.ExpectedVariables;
import se.liu.ida.hefquin.engine.queryplan.executable.NaryExecutableOp;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ops.ExecOpFrawMultiwayUnion;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ops.ExecOpMultiwayUnion;
import se.liu.ida.hefquin.engine.queryplan.info.QueryPlanningInfo;
import se.liu.ida.hefquin.engine.queryplan.physical.NaryPhysicalOp;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalPlanVisitor;

public class PhysicalOpFrawMultiwayUnion extends PhysicalOpMultiwayUnion implements NaryPhysicalOp {

    @Override
    public NaryExecutableOp createExecOp( final boolean collectExceptions,
                                          final QueryPlanningInfo qpInfo,
                                          final ExpectedVariables... inputVars) {
        return new ExecOpFrawMultiwayUnion( inputVars.length, collectExceptions, qpInfo );
    }

    @Override
    public void visit(PhysicalPlanVisitor visitor) {
        visitor.visit(this);
    }
}