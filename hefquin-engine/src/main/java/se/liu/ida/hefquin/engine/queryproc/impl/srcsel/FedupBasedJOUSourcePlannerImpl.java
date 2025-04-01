package se.liu.ida.hefquin.engine.queryproc.impl.srcsel;

import org.apache.jena.sparql.algebra.Op;
import se.liu.ida.hefquin.base.utils.Pair;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlan;
import se.liu.ida.hefquin.engine.queryproc.QueryProcContext;
import se.liu.ida.hefquin.engine.queryproc.SourcePlanningException;
import se.liu.ida.hefquin.engine.queryproc.SourcePlanningStats;

import java.util.Objects;

public class FedupBasedJOUSourcePlannerImpl extends FedupBasedSourcePlannerImpl {

    public FedupBasedJOUSourcePlannerImpl(QueryProcContext qpc, String summaries, String lambdaString){
        super(qpc, summaries, lambdaString);
    }

    @Override
    protected Pair<LogicalPlan, SourcePlanningStats> createSourceAssignment(Op jenaOp) throws SourcePlanningException {
        return super.createSourceAssignment(jenaOp);
    }

    @Override
    protected Op getSourceSelection(Op jenaOp) throws SourcePlanningException {
        Op op = super.getSourceSelection(jenaOp);
        return new JOUConverter().convert(op);
    }

    protected LogicalPlan createPlan(Op op) {
        if(Objects.isNull(op)) return null;
        return super.createPlan(op);
    }

}