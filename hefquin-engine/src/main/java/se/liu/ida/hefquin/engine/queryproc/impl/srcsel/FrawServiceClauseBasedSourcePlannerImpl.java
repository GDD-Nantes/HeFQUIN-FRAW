package se.liu.ida.hefquin.engine.queryproc.impl.srcsel;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.BasicPattern;
import se.liu.ida.hefquin.base.query.impl.QueryPatternUtils;
import se.liu.ida.hefquin.engine.federation.FederationMember;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlan;
import se.liu.ida.hefquin.engine.queryproc.QueryProcContext;

public class FrawServiceClauseBasedSourcePlannerImpl extends ServiceClauseBasedSourcePlannerImpl{
    public FrawServiceClauseBasedSourcePlannerImpl(QueryProcContext ctxt) {
        super(ctxt);
    }

    @Override
    protected LogicalPlan createPlanForBGP(final BasicPattern pattern, final FederationMember fm ) {
        return createPlanForBGP( QueryPatternUtils.createBGP(pattern), fm );
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

}
