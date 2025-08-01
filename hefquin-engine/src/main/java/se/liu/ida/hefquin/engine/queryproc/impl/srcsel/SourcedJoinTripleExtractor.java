package se.liu.ida.hefquin.engine.queryproc.impl.srcsel;

import fr.gdd.fedqpl.visitors.OpVisitorUnimplemented;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.expr.Expr;
import se.liu.ida.hefquin.engine.OpVisitorRouter;

import java.util.HashSet;
import java.util.Set;


// This class extracts triples and filters from all operators made of join-like (BGP, triple, sequence, join), filters, and service operators
public class SourcedJoinTripleExtractor extends OpVisitorUnimplemented {
    Set<Triple> triples = new HashSet<>();
    Set<Expr> filters = new HashSet<>();

    public void visit(OpJoin op) {
        OpVisitorRouter.visit(this, op.getLeft());
        OpVisitorRouter.visit(this, op.getRight());
    }

    public void visit(OpTriple op) {
        this.triples.add(op.getTriple());
    }

    public void visit(OpBGP op) {
        this.triples.addAll(op.getPattern().getList());
    }

    public void visit(OpSequence op) {
        op.getElements().forEach(e -> OpVisitorRouter.visit(this, e));
    }

    public void visit(OpFilter op) {
        filters.addAll(op.getExprs().getList());
        OpVisitorRouter.visit(this, op.getSubOp());
    }

    public void visit(OpService op) {
        OpVisitorRouter.visit(this, op.getSubOp());
    }

    public Set<Triple> getTriples() { return triples; }
    public Set<Expr> getFilters() { return filters; }
}
