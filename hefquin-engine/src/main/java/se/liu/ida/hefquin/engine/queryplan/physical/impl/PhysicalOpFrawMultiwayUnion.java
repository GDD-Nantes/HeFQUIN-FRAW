package se.liu.ida.hefquin.engine.queryplan.physical.impl;

import se.liu.ida.hefquin.base.queryplan.ExpectedVariables;
import se.liu.ida.hefquin.engine.queryplan.executable.NaryExecutableOp;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ops.ExecOpFrawMultiwayUnion;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalOperator;
import se.liu.ida.hefquin.engine.queryplan.physical.NaryPhysicalOp;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalPlanVisitor;

public class PhysicalOpFrawMultiwayUnion extends PhysicalOpMultiwayUnion implements NaryPhysicalOp {

    @Override
    public NaryExecutableOp createExecOp(boolean collectExceptions, ExpectedVariables... inputVars) {
        return new ExecOpFrawMultiwayUnion(inputVars.length, collectExceptions);
    }

    @Override
    public void visit(PhysicalPlanVisitor visitor) {
        visitor.visit(this);
    }
}
