package se.liu.ida.hefquin.engine.queryplan.physical.impl;

import se.liu.ida.hefquin.base.query.ExpectedVariables;
import se.liu.ida.hefquin.engine.queryplan.executable.BinaryExecutableOp;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ops.ExecOpFrawBinaryUnion;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalOpUnion;
import se.liu.ida.hefquin.engine.queryplan.physical.BinaryPhysicalOp;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalPlanVisitor;

public class PhysicalOpFrawBinaryUnion extends PhysicalOpBinaryUnion implements BinaryPhysicalOp {

    public PhysicalOpFrawBinaryUnion(LogicalOpUnion lop) {
        super(lop);
    }

    @Override
    public BinaryExecutableOp createExecOp(boolean collectExceptions, ExpectedVariables... inputVars) {
        return new ExecOpFrawBinaryUnion(collectExceptions);
    }

    @Override
    public void visit(PhysicalPlanVisitor visitor) {
        visitor.visit(this);
    }
}