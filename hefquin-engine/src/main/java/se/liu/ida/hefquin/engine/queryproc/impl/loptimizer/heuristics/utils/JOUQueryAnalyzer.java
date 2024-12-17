package se.liu.ida.hefquin.engine.queryproc.impl.loptimizer.heuristics.utils;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import se.liu.ida.hefquin.base.query.TriplePattern;
import se.liu.ida.hefquin.engine.federation.FederationMember;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalOperator;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlan;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.*;
import se.liu.ida.hefquin.engine.queryplan.utils.LogicalOpUtils;

import java.util.HashSet;
import java.util.Set;

public class JOUQueryAnalyzer {
    MultiValuedMap<TriplePattern, FederationMember> tpsl = new HashSetValuedHashMap<>();


    public JOUQueryAnalyzer(LogicalPlan plan) {
        extractTriplePatternBasedSourceSelection(plan);
    }

    protected Set<TriplePattern> extractTriplePatternBasedSourceSelection( final LogicalPlan plan ) {
        final LogicalOperator lop = plan.getRootOperator();

        if( lop instanceof LogicalOpRequest) {
            FederationMember fm = ((LogicalOpRequest<?, ?>) lop).getFederationMember();
            Set<TriplePattern> ret = LogicalOpUtils.getTriplePatternsOfReq( (LogicalOpRequest<?, ?>) lop);
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

}
