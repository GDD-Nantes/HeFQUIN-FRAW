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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class JOUConverterUtils {

    public static boolean isJoinOfRequest(LogicalPlan lp){
        for(int i = 0; i < lp.numberOfSubPlans(); i++){
            if(! (lp.getSubPlan(i).getRootOperator() instanceof LogicalOpRequest<?,?>))
                return false;
        }

        return true;
    }

    public static boolean isSimpleUnionOverJoin(LogicalPlan lp){

        LogicalOperator lop = lp.getRootOperator();

        if( lop instanceof LogicalOpFilter) return true;

        if( lop instanceof LogicalOpRequest<?,?> ) return true;

        if( ! (lop instanceof LogicalOpUnion || lop instanceof LogicalOpMultiwayUnion))
            return false;

        for(int i = 0; i < lp.numberOfSubPlans(); i++){
            if(! isJoinOfRequest(lp.getSubPlan(i)))
                return false;
        }

        return true;
    }

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
            if(filterExpressions.addAll(filter.getFilterExpressions().getList()));
        }

        if(filterExpressions.isEmpty()){
            // there are no filters in the plan, we just return the join of unions
            return rootPlanJoin;
        }

        // there are filters to apply, we create a filter operator atop the root join to create a plan that we return.
        // this is always semantically correct, and filter push down optimizations are applied later
        return new LogicalPlanWithUnaryRootImpl(new LogicalOpFilter(ExprList.create(filterExpressions)), rootPlanJoin);
    }

    public static LogicalPlan unionOverJoin2JoinOverUnion(LogicalPlan lp){

        if ( isSimpleUnionOverJoin(lp) ) {
            return simpleUnionOverJoin2JoinOverUnion(lp);
        }

        if(lp.getRootOperator() instanceof LogicalOpMultiwayUnion){
            List<LogicalPlan> subPlans = new ArrayList<>();

            for(int i = 0; i < lp.numberOfSubPlans(); i++){
                LogicalPlan subPlan = unionOverJoin2JoinOverUnion(lp.getSubPlan(i));
                subPlans.add(subPlan);
            }

            return new LogicalPlanWithNaryRootImpl(LogicalOpMultiwayUnion.getInstance(), subPlans);
        }

        throw new IllegalArgumentException("Logical Plan " + lp + " is not a well-formed fedup union over join plan");
    }
}
