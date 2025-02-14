package se.liu.ida.hefquin.engine.queryproc.impl.loptimizer.heuristics.utils;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.apache.jena.sparql.expr.ExprList;
import se.liu.ida.hefquin.base.query.TriplePattern;
import se.liu.ida.hefquin.engine.federation.FederationMember;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalOperator;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlan;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalOpFilter;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalOpMultiwayJoin;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalOpMultiwayUnion;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalOpRequest;
import se.liu.ida.hefquin.engine.queryplan.utils.LogicalOpUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class JOUQueryAnalyzer {
    MultiValuedMap<TriplePattern, FederationMember> tpsl = new HashSetValuedHashMap<>();
    List<LogicalOpFilter> filters = new ArrayList<>();


    public JOUQueryAnalyzer(LogicalPlan plan) {
        extractTriplePatternBasedSourceSelection(plan);
    }

    protected Set<TriplePattern> extractTriplePatternBasedSourceSelection( final LogicalPlan plan ) {
        final LogicalOperator lop = plan.getRootOperator();

        if( lop instanceof LogicalOpRequest) {
            FederationMember fm = ((LogicalOpRequest<?, ?>) lop).getFederationMember();
            ((LogicalOpRequest<?,?>) lop).getRequest();
            Set<TriplePattern> ret = LogicalOpUtils.getTriplePatternsOfReq( (LogicalOpRequest<?, ?>) lop );
            Set<ExprList> exprLists = LogicalOpUtils.getFilterExprsOfReq( (LogicalOpRequest<?, ?>) lop );
            exprLists.stream().forEach(el -> filters.add(new LogicalOpFilter(el)));
            ret.stream().forEach(
                    tp -> tpsl.put( tp, fm )
            );
            return ret;
        } else if ( lop instanceof LogicalOpMultiwayJoin ) {
            Set<TriplePattern> ret = new HashSet<>();
            for( int i=0; i<plan.numberOfSubPlans(); i++ ) {
                ret.addAll(extractTriplePatternBasedSourceSelection(plan.getSubPlan(i)));
            }
            return ret;
        }else if ( lop instanceof LogicalOpMultiwayUnion ) {
            Set<TriplePattern> ret = new HashSet<>();
            for( int i=0; i<plan.numberOfSubPlans(); i++ ) {
                ret.addAll(extractTriplePatternBasedSourceSelection(plan.getSubPlan(i)));
            }
            return ret;
        } else if( lop instanceof LogicalOpFilter ) {
            filters.add(((LogicalOpFilter) lop));
            return extractTriplePatternBasedSourceSelection( plan.getSubPlan(0) );
        }
        else
            // TODO : handle optional / left joins
            throw new IllegalArgumentException("Unsupported type of root operator (" + lop.getClass().getName() + ")");
    }

    public Set<TriplePattern> getTps(){
        return tpsl.keySet();
    }

    public MultiValuedMap<TriplePattern, FederationMember> getTpsl(){
        return tpsl;
    }

    public List<LogicalOpFilter> getFilters() {
        return filters;
    }
}
