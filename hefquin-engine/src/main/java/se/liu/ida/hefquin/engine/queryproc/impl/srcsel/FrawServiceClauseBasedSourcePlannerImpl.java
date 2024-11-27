package se.liu.ida.hefquin.engine.queryproc.impl.srcsel;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.BasicPattern;
import se.liu.ida.hefquin.base.query.impl.QueryPatternUtils;
import se.liu.ida.hefquin.engine.federation.FederationMember;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlan;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalOpMultiwayJoin;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalPlanWithNaryRootImpl;
import se.liu.ida.hefquin.engine.queryproc.QueryProcContext;

import java.util.ArrayList;
import java.util.List;

public class FrawServiceClauseBasedSourcePlannerImpl extends ServiceClauseBasedSourcePlannerImpl{
    public FrawServiceClauseBasedSourcePlannerImpl(QueryProcContext ctxt) {
        super(ctxt);
    }

    @Override
    protected LogicalPlan createPlanForBGP(final BasicPattern pattern, final FederationMember fm ) {
        return createPlanForBGP( QueryPatternUtils.createFrawBGP(pattern), fm );
    }

    @Override
    protected LogicalPlan createPlan( final Op jenaOp ) {
        if ( jenaOp instanceof OpSequence) {
            return createPlanForSequence( (OpSequence) jenaOp );
        }
        else if ( jenaOp instanceof OpJoin) {
            return createPlanForJoin( (OpJoin) jenaOp );
        }
        else if ( jenaOp instanceof OpLeftJoin) {
            return createPlanForLeftJoin( (OpLeftJoin) jenaOp );
        }
        else if ( jenaOp instanceof OpConditional) {
            return createPlanForLeftJoin( (OpConditional) jenaOp );
        }
        else if ( jenaOp instanceof OpUnion) {
            return createPlanForUnion( (OpUnion) jenaOp );
        }
        else if ( jenaOp instanceof OpFilter ) {
            return createPlanForFilter( (OpFilter) jenaOp );
        }
        else if ( jenaOp instanceof OpExtend ) {
            return createPlanForBind( (OpExtend) jenaOp );
        }
        else if ( jenaOp instanceof OpService ) {
            return createPlanForServicePattern( (OpService) jenaOp );
        }
        else {
            throw new IllegalArgumentException( "unsupported type of query pattern: " + jenaOp.getClass().getName() );
        }
    }

    protected LogicalPlan mergeIntoMultiwayJoin( final List<LogicalPlan> subPlans ) {
        if ( subPlans.size() == 1 ) {
            return subPlans.get(0);
        }

        final List<LogicalPlan> subPlansFlattened = new ArrayList<>();

        for ( final LogicalPlan subPlan : subPlans ) {
            if ( subPlan.getRootOperator() instanceof LogicalOpMultiwayJoin) {
                for ( int j = 0; j < subPlan.numberOfSubPlans(); ++j ) {
                    subPlansFlattened.add( subPlan.getSubPlan(j) );
                }
            }
            else {
                subPlansFlattened.add( subPlan );
            }
        }

        return new LogicalPlanWithNaryRootImpl( LogicalOpMultiwayJoin.getInstance(),
                subPlansFlattened );
    }

}
