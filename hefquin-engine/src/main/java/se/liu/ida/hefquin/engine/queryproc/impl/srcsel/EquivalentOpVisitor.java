package se.liu.ida.hefquin.engine.queryproc.impl.srcsel;

import fr.gdd.fedqpl.visitors.ReturningArgsOpVisitor;
import fr.gdd.fedqpl.visitors.ReturningArgsOpVisitorRouter;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;

public class EquivalentOpVisitor extends ReturningArgsOpVisitor<Boolean, Op> {

    public Boolean visit(OpJoin op, Op other) {
        if( ! (other instanceof OpJoin) ) {
            return false;
        }

        return ReturningArgsOpVisitorRouter.visit(this, op.getLeft(), ((OpJoin) other).getLeft())
            && ReturningArgsOpVisitorRouter.visit(this, op.getRight(), ((OpJoin) other).getRight());
    }

    public Boolean visit(OpLeftJoin op, Op other) {
        if( ! (other instanceof OpLeftJoin) ) {
            return false;
        }

        return ReturningArgsOpVisitorRouter.visit(this, op.getLeft(), ((OpLeftJoin) other).getLeft())
                && ReturningArgsOpVisitorRouter.visit(this, op.getRight(), ((OpLeftJoin) other).getRight());
    }

    public Boolean visit(OpUnion op, Op other) {
        if( ! (other instanceof OpUnion) ) {
            return false;
        }

        return ReturningArgsOpVisitorRouter.visit(this, op.getLeft(), ((OpUnion) other).getLeft())
                && ReturningArgsOpVisitorRouter.visit(this, op.getRight(), ((OpUnion) other).getRight());
    }

    public Boolean visit(OpBGP op, Op other) {
        if( other instanceof OpTriple ) return ((OpTriple) other).equivalent(op);

        if( ! (other instanceof OpBGP) ) {
            return false;
        }

        return op.equalTo(other, null);
    }

    public Boolean visit(OpTriple op, Op other) {
        if( other instanceof OpBGP ) return op.equivalent((OpBGP) other);

        return op.equalTo(other, null);
    }

    public Boolean visit(OpService op, Op other) {
        if( ! (other instanceof OpService) ) {
            return false;
        }

        return ReturningArgsOpVisitorRouter.visit(this, op.getSubOp(), ((OpService) other).getSubOp());
    }

    public Boolean visit(OpSequence op, Op other) {
        if( ! (other instanceof OpSequence) ) {
            return false;
        }

        for( int i=0; i < op.size(); i++ ) {
            Op subOp = op.get(i);
            Op otherSubOp = ((OpSequence) other).get(i);
            if( ! ReturningArgsOpVisitorRouter.visit(this, subOp, otherSubOp ) ) return false;
        }

        return true;
    }

    public Boolean visit(OpFilter op, Op other) {
        if( ! (other instanceof OpFilter) ) {
            return false;
        }

        OpFilter otherFilter = (OpFilter) other;

        if ( ! op.getExprs().equals( otherFilter.getExprs() ) ) return false;

        return ReturningArgsOpVisitorRouter.visit(this, op.getSubOp(), ((Op1) other).getSubOp());
    }
}
