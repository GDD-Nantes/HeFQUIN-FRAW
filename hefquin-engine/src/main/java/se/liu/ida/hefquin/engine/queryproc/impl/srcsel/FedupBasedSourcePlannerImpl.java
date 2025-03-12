package se.liu.ida.hefquin.engine.queryproc.impl.srcsel;

import fr.gdd.fedup.FedUP;
import fr.gdd.fedup.summary.ModuloOnSuffix;
import fr.gdd.fedup.summary.Summary;
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpProject;
import se.liu.ida.hefquin.base.utils.Pair;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlan;
import se.liu.ida.hefquin.engine.queryproc.QueryProcContext;
import se.liu.ida.hefquin.engine.queryproc.SourcePlanningException;
import se.liu.ida.hefquin.engine.queryproc.SourcePlanningStats;

import java.nio.file.Path;
import java.util.Objects;
import java.util.function.Function;

public class FedupBasedSourcePlannerImpl extends ServiceClauseBasedSourcePlannerImpl {

    FedUP fedup;
    public FedupBasedSourcePlannerImpl(QueryProcContext qpc, String summaries, String lambdaString){
        super(qpc);

        Summary summary = new Summary(new ModuloOnSuffix(1), Location.create(Path.of(summaries)));
        fedup = new FedUP(summary);

        Function<String, String> lambda =
                InMemoryLambdaJavaFileObject.getLambda("ModifyEndpoints",
                        lambdaString, "String");
        if (Objects.isNull(lambda)) {
            throw new UnsupportedOperationException("The lambda expression does not seem valid.");
        }
        fedup.modifyEndpoints(lambda);
    }

    @Override
    protected Pair<LogicalPlan, SourcePlanningStats> createSourceAssignment(Op jenaOp) throws SourcePlanningException {
        Op op = getFedupSourceSelection(jenaOp);
        if(Objects.isNull(op)) return null;
        final LogicalPlan sa = createPlan(op);
        final SourcePlanningStats myStats = new SourcePlanningStatsImpl();

        return new Pair<>(sa, myStats);
    }

    protected LogicalPlan createPlan(Op op) {
        if(Objects.isNull(op)) {
//            throw new UnsupportedOperationException("The op expression does not seem valid.");
            // TODO : handle null op from source assignment where no source combination produces results
        }
        if(op instanceof OpProject){
            return super.createPlan(((OpProject) op).getSubOp());
        }
        return super.createPlan(op);
    }

    protected Op getFedupSourceSelection(Op jenaOp){
        Op ret = fedup.queryJenaToJena(jenaOp);
        return ret;
    }
}
