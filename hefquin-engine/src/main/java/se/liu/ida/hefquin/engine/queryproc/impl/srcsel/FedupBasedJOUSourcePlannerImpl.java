package se.liu.ida.hefquin.engine.queryproc.impl.srcsel;

import org.apache.jena.sparql.algebra.Op;
import se.liu.ida.hefquin.base.utils.Pair;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlan;
import se.liu.ida.hefquin.engine.queryproc.QueryProcContext;
import se.liu.ida.hefquin.engine.queryproc.SourcePlanningException;
import se.liu.ida.hefquin.engine.queryproc.SourcePlanningStats;

import java.util.Objects;

public class FedupBasedJOUSourcePlannerImpl extends FedupBasedSourcePlannerImpl{

    public FedupBasedJOUSourcePlannerImpl(QueryProcContext qpc, String summaries, String lambdaString) {
        super(qpc, summaries, lambdaString);
    }

    @Override
    protected Pair<LogicalPlan, SourcePlanningStats> createSourceAssignment(Op jenaOp) throws SourcePlanningException {
        Op op = getFedupSourceSelection(jenaOp);

        if(Objects.isNull(op)) return null;

        final LogicalPlan sa = createPlan(op);

        final LogicalPlan jou = JOUConverterUtils.unionOverJoin2JoinOverUnion(sa);

        final SourcePlanningStats myStats = new SourcePlanningStatsImpl();

        return new Pair<>(jou, myStats);
    }
}
