package se.liu.ida.hefquin.engine.queryproc.impl.srcsel;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprList;
import se.liu.ida.hefquin.base.query.TriplePattern;
import se.liu.ida.hefquin.engine.federation.FederationMember;
import se.liu.ida.hefquin.engine.federation.access.SPARQLRequest;
import se.liu.ida.hefquin.engine.federation.access.impl.req.SPARQLRequestImpl;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalOperator;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlan;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.*;
import se.liu.ida.hefquin.engine.queryproc.impl.loptimizer.heuristics.utils.JOUQueryAnalyzer;

import java.util.*;
import java.util.function.BiFunction;

public class JOUConverterUtils {

    public static LogicalPlan simpleUnionOverJoin2JoinOverUnion(LogicalPlan lp){
        JOUQueryAnalyzer eqa = new JOUQueryAnalyzer(lp);
        MultiValuedMap<TriplePattern, FederationMember> tpsl = eqa.getTpsl();
        List<LogicalOpFilter> filters = eqa.getFilters();

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

        Set<Expr> filterExpressions = new HashSet<>();
        for(LogicalOpFilter filter : filters){
            filterExpressions.addAll(filter.getFilterExpressions().getList());
        }

        if(filterExpressions.isEmpty()){
            // there are no filters in the plan, we just return the join of unions
            return rootPlanJoin;
        }

        // there are filters to apply, we create a filter operator atop the root join to create a plan that we return.
        // this is always semantically correct, and filter push down optimizations are applied later
        return new LogicalPlanWithUnaryRootImpl(new LogicalOpFilter(ExprList.create(filterExpressions)), rootPlanJoin);
    }







    // ################

    public static LogicalPlan unionOverJoin2JoinOverUnion(LogicalPlan lp){
        LogicalOperator lop = lp.getRootOperator();

        if(lop instanceof LogicalOpMultiwayUnion){
            List<List<LogicalPlan>> groups = getIdenticalLogicalPlans(lp);

            if(groups.size() == 1) return simpleUnionOverJoin2JoinOverUnion(lp);

            List<LogicalPlan> jous = new ArrayList<>();

            for(List<LogicalPlan> group : groups){
                LogicalPlan subPlan = new LogicalPlanWithNaryRootImpl(LogicalOpMultiwayUnion.getInstance(), group);

                LogicalPlan jou = simpleUnionOverJoin2JoinOverUnion(subPlan);

                jous.add(jou);
            }

            return new LogicalPlanWithNaryRootImpl(LogicalOpMultiwayUnion.getInstance(), jous);
        }

        if(lop instanceof LogicalOpFilter){
            return new LogicalPlanWithUnaryRootImpl(new LogicalOpFilter(((LogicalOpFilter) lop).getFilterExpressions()), lp.getSubPlan(0));
        }

        if(lop instanceof LogicalOpRequest<?,?>){
            return lp;
        }

        throw new UnsupportedOperationException("LogicalPlan " + lp + " isn't a well-formed fedup union over join plan");
    }

    public static List<List<LogicalPlan>> getIdenticalLogicalPlans(LogicalPlan lp){
        if(!(lp.getRootOperator() instanceof LogicalOpMultiwayUnion)) throw new IllegalArgumentException("Can't extract identical joins of a union from a non union logical plan");

        EquivalentSetMap<LogicalPlan> esm = new EquivalentSetMap<>(JOUConverterUtils::arePatternEquivalent);

        for(int i = 0; i<lp.numberOfSubPlans(); i++){
            LogicalPlan subPlan = lp.getSubPlan(i);
            esm.put(subPlan);
        }

        //Collection of set
        return esm.map.values().stream().toList();
    }

    public static boolean arePatternEquivalent(LogicalPlan lp1, LogicalPlan lp2){
        if(lp1.getRootOperator().getClass() != lp2.getRootOperator().getClass()) return false;
        if(lp1.numberOfSubPlans() != lp2.numberOfSubPlans()) return false;

        if(lp1.getRootOperator() instanceof LogicalOpRequest<?,?>){
            return ((LogicalOpRequest<?, ?>) lp1.getRootOperator()).getRequest().equals(((LogicalOpRequest<?, ?>) lp2.getRootOperator()).getRequest());
        }

        if(lp1.numberOfSubPlans() == 0 || lp2.numberOfSubPlans() == 0) {
            // Both plans are logical plans with a nullary root but are not requests... this shouldn't happen
            throw new RuntimeException("Logical Plan " + lp1 + " and Logical Plan " + lp2 + " are not request operators," +
                    " yet at least one of them do not contain any children ");
        }

        for(int i = 0; i < lp1.numberOfSubPlans(); i++){
            if(!arePatternEquivalent(lp1.getSubPlan(i), lp2.getSubPlan(i))) return false;
        }

        return false;
    }

    private static class EquivalentSetMap<K> {
        Map<K, List<K>> map;
        BiFunction<K, K, Boolean> equivalent;

        public EquivalentSetMap(BiFunction<K, K, Boolean> f) {
            this.map = new HashMap<>();
            this.equivalent = f;
        }

        public void put(K key) {
            boolean added = false;

            for(K k : map.keySet()) {
                if(added) break;
                if(equivalent.apply(k, key)) added = map.get(k).add(key);
            }

            if(!added) {
                List list = new ArrayList();
                list.add(key);
                map.put(key, list);
            }
        }
    }
}
