package se.liu.ida.hefquin.engine.queryproc.impl.srcsel;

import fr.gdd.fedup.FedUP;
import fr.gdd.fedup.summary.ModuloOnSuffix;
import fr.gdd.fedup.summary.Summary;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.apache.commons.logging.Log;
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpProject;
import se.liu.ida.hefquin.base.query.TriplePattern;
import se.liu.ida.hefquin.base.utils.Pair;
import se.liu.ida.hefquin.engine.federation.FederationMember;
import se.liu.ida.hefquin.engine.federation.access.SPARQLRequest;
import se.liu.ida.hefquin.engine.federation.access.impl.req.SPARQLRequestImpl;
import se.liu.ida.hefquin.engine.federation.access.utils.SPARQLRequestUtils;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalOperator;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlan;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlanWithNullaryRoot;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.*;
import se.liu.ida.hefquin.engine.queryproc.QueryProcContext;
import se.liu.ida.hefquin.engine.queryproc.SourcePlanningException;
import se.liu.ida.hefquin.engine.queryproc.SourcePlanningStats;
import se.liu.ida.hefquin.engine.queryproc.impl.loptimizer.heuristics.utils.JOUQueryAnalyzer;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class FedupBasedJOUSourcePlannerImpl extends ServiceClauseBasedSourcePlannerImpl{
    FedUP fedup;

    public FedupBasedJOUSourcePlannerImpl(QueryProcContext qpc) {
        super(qpc);

        // Hardcoded, TODO : make it dependent
        Summary summary = new Summary(new ModuloOnSuffix(1), Location.create(Path.of("fedshop20-h0")));
        fedup = new FedUP(summary);

        // Hardcoded, TODO : make it config dependent
        Function<String, String> lambda = (e)->"http://localhost:3331/blazegraph.jnl/raw?default-graph-uri="+(e);
        fedup.modifyEndpoints(lambda);
    }

    @Override
    protected Pair<LogicalPlan, SourcePlanningStats> createSourceAssignment(Op jenaOp) throws SourcePlanningException {
        Op op = fedup.queryJenaToJena(jenaOp);

        final LogicalPlan sa = createPlan(op);

        final LogicalPlan jou = unionOverJoin2JoinOverUnion(sa);

        final SourcePlanningStats myStats = new SourcePlanningStatsImpl();

        return new Pair<>(jou, myStats);
    }

    protected LogicalPlan createPlan(Op op) {
        if(op instanceof OpProject){
            return super.createPlan(((OpProject) op).getSubOp());
        }
        return super.createPlan(op);
    }

    protected LogicalPlan unionOverJoin2JoinOverUnion(LogicalPlan lp){
        // TODO : handle optionals / left join
        JOUQueryAnalyzer eqa = new JOUQueryAnalyzer(lp);
        MultiValuedMap<TriplePattern, FederationMember> tpsl = eqa.getTpsl();

        List<LogicalPlan> rootJoinChildren = new ArrayList<>();

        for (TriplePattern tp : tpsl.keySet()) {

            List<LogicalPlan> childUnionChildren = new ArrayList<>();

            for (FederationMember member : tpsl.get(tp)) {

                SPARQLRequest req = new SPARQLRequestImpl(tp);

                LogicalPlan subPlanTP = new LogicalPlanWithNullaryRootImpl(new LogicalOpRequest<>(member, req));

                childUnionChildren.add(subPlanTP);
            }

            LogicalPlan subPlanUnion =
                    new LogicalPlanWithNaryRootImpl(LogicalOpMultiwayUnion.getInstance(), childUnionChildren);

            rootJoinChildren.add(subPlanUnion);

        }

        LogicalPlan rootPlanJoin =
                new LogicalPlanWithNaryRootImpl(LogicalOpMultiwayJoin.getInstance(), rootJoinChildren);

        return rootPlanJoin;
    }
}
