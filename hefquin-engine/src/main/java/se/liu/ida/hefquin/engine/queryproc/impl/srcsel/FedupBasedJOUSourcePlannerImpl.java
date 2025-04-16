package se.liu.ida.hefquin.engine.queryproc.impl.srcsel;

import org.apache.jena.sparql.algebra.Op;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlan;
import se.liu.ida.hefquin.engine.queryproc.QueryProcContext;
import se.liu.ida.hefquin.engine.queryproc.SourcePlanningException;

import java.util.Objects;

public class FedupBasedJOUSourcePlannerImpl extends FedupBasedSourcePlannerImpl {

    public FedupBasedJOUSourcePlannerImpl(QueryProcContext qpc, String summaries, String lambdaString){
        super(qpc, summaries, lambdaString);
    }

    @Override
    protected Op getSourceSelection(Op jenaOp) throws SourcePlanningException {
        System.out.println("JEVAISDEVENIRFOUUUUUUUU");
        Op op = super.getSourceSelection(jenaOp);
        System.out.println("DIZODEIDQKSJHFQBFKQJSDBHFQDKSJHFJQKDHFLQHLF");
        return new JOUConverter().convert(op);
    }

    protected LogicalPlan createPlan(Op op) {
        if(Objects.isNull(op)) return null;
        return super.createPlan(op);
    }

}