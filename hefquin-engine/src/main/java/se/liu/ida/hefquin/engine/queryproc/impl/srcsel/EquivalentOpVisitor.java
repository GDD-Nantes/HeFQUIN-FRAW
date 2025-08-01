package se.liu.ida.hefquin.engine.queryproc.impl.srcsel;

import fr.gdd.fedqpl.visitors.ReturningArgsOpVisitor;
import fr.gdd.fedqpl.visitors.ReturningArgsOpVisitorRouter;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import se.liu.ida.hefquin.engine.OpVisitorRouter;

public class EquivalentOpVisitor extends ReturningArgsOpVisitor<Boolean, Op> {

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

    public Boolean visit(OpBGP op, Op other){
        return areJoinEquivalent(op, other);
    }

    public Boolean visit(OpTriple op, Op other) {
        return areJoinEquivalent(op, other);
    }

    public Boolean visit(OpService op, Op other) {
        return areJoinEquivalent(op, other);
    }

    public Boolean visit(OpSequence op, Op other) {
        return areJoinEquivalent(op, other);
    }

    public Boolean visit(OpFilter op, Op other) {
        return areJoinEquivalent(op, other);
    }

    public Boolean visit(OpJoin op, Op other) {
        return areJoinEquivalent(op, other);
    }

    private static boolean areJoinEquivalent(Op op1, Op op2) {
        try{
            SourcedJoinTripleExtractor extractorOp1 = new SourcedJoinTripleExtractor();
            OpVisitorRouter.visit(extractorOp1, op1);

            SourcedJoinTripleExtractor extractorOp2 = new SourcedJoinTripleExtractor();
            OpVisitorRouter.visit(extractorOp2, op2);

            boolean triplesEqual = extractorOp1.getTriples().equals(extractorOp2.getTriples());
            boolean filtersEqual = extractorOp1.getFilters().equals(extractorOp2.getFilters());

            return triplesEqual && filtersEqual;
        }catch (UnsupportedOperationException e){
            return false;
        }
    }
}
