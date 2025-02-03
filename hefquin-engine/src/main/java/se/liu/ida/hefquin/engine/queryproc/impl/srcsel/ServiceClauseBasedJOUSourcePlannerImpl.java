package se.liu.ida.hefquin.engine.queryproc.impl.srcsel;

import org.apache.jena.sparql.algebra.Op;
import se.liu.ida.hefquin.base.utils.Pair;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlan;
import se.liu.ida.hefquin.engine.queryproc.QueryProcContext;
import se.liu.ida.hefquin.engine.queryproc.SourcePlanningException;
import se.liu.ida.hefquin.engine.queryproc.SourcePlanningStats;

public class ServiceClauseBasedJOUSourcePlannerImpl extends ServiceClauseBasedSourcePlannerImpl {

    public ServiceClauseBasedJOUSourcePlannerImpl(QueryProcContext ctxt) {
        super(ctxt);
    }

    @Override
    protected Pair<LogicalPlan, SourcePlanningStats> createSourceAssignment(Op jenaOp) throws SourcePlanningException {
        Pair<LogicalPlan, SourcePlanningStats> pair = super.createSourceAssignment(jenaOp);

        LogicalPlan uoj = pair.object1;

        final LogicalPlan jou = JOUConverterUtils.unionOverJoin2JoinOverUnion(uoj);

        return new Pair<>(jou, pair.object2);
    }
}
