package se.liu.ida.hefquin.engine.queryplan.physical.impl;

import se.liu.ida.hefquin.base.queryplan.ExpectedVariables;
import se.liu.ida.hefquin.engine.queryplan.executable.BinaryExecutableOp;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ops.ExecOpFrawNaiveNestedLoopsJoin;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ops.ExecOpNaiveNestedLoopsJoin;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalOpJoin;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalPlanVisitor;

public class PhysicalOpFrawJoin extends BaseForPhysicalOpBinaryJoin{

    public PhysicalOpFrawJoin(LogicalOpJoin lop) {
        super(lop);
    }

    @Override
    public BinaryExecutableOp createExecOp(boolean collectExceptions, ExpectedVariables... inputVars) {
        return new ExecOpFrawNaiveNestedLoopsJoin(collectExceptions);
    }

    @Override
    public void visit(PhysicalPlanVisitor visitor) {

    }
}
