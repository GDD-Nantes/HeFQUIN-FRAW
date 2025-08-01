package se.liu.ida.hefquin.engine.queryproc.impl.srcsel;

import org.apache.jena.sparql.algebra.Op;
import se.liu.ida.hefquin.engine.queryproc.QueryProcContext;
import se.liu.ida.hefquin.engine.queryproc.SourcePlanningException;

public class FedupBasedJOUSourcePlannerImpl extends FedupBasedSourcePlannerImpl {

    public FedupBasedJOUSourcePlannerImpl(QueryProcContext qpc, String summaries, String lambdaString){
        super(qpc, summaries, lambdaString);
    }

    @Override
    protected Op getSourceSelection(Op jenaOp) throws SourcePlanningException {
        Op op = super.getSourceSelection(jenaOp);
        return new JOUConverter().convert(op);
    }
}