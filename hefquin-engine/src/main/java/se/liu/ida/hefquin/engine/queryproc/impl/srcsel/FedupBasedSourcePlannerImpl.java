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
    int nbResults;

    public FedupBasedSourcePlannerImpl(QueryProcContext qpc){
        super(qpc);
        Summary summary = new Summary(new ModuloOnSuffix(1), Location.create(Path.of("fedshop200-h0")));
        fedup = new FedUP(summary);

        // Hardcoded, TODO : make it config dependent
        Function<String, String> lambda = (e)->"http://localhost:3331/blazegraph.jnl/raw?default-graph-uri="+(e);
        fedup.modifyEndpoints(lambda);
    }

    @Override
    protected Pair<LogicalPlan, SourcePlanningStats> createSourceAssignment(Op jenaOp) throws SourcePlanningException {
        Op op = fedup.queryJenaToJena(jenaOp);



        // TODO : add limits to every service clause : )

        final LogicalPlan sa = createPlan(op);
        if(Objects.isNull(op)) return null;
        final SourcePlanningStats myStats = new SourcePlanningStatsImpl();

        return new Pair<>(sa, myStats);
    }

    protected LogicalPlan createPlan(Op op) {
        if(Objects.isNull(op)) return null;
        if(op instanceof OpProject){
            return super.createPlan(((OpProject) op).getSubOp());
        }
        return super.createPlan(op);
    }
}
