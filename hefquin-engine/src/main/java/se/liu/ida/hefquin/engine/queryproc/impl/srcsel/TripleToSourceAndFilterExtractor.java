package se.liu.ida.hefquin.engine.queryproc.impl.srcsel;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.expr.Expr;
import se.liu.ida.hefquin.engine.OpVisitorRouter;

import java.util.HashSet;
import java.util.Set;

public class TripleToSourceAndFilterExtractor extends OpVisitorBase {
    MultiValuedMap<Triple, Node> triples2Source = new HashSetValuedHashMap<>();
    Set<Expr> filters = new HashSet<>();

    public void visit(OpJoin op) {
        OpVisitorRouter.visit(this, op.getLeft());
        OpVisitorRouter.visit(this, op.getRight());
    }

    public void visit(OpLeftJoin op) {
        OpVisitorRouter.visit(this, op.getLeft());
        OpVisitorRouter.visit(this, op.getRight());
    }

    public void visit(OpUnion op) {
        OpVisitorRouter.visit(this, op.getLeft());
        OpVisitorRouter.visit(this, op.getRight());
    }

    public void visit(OpService op) {
        ServiceContentExtractor sce = new ServiceContentExtractor();
        OpVisitorRouter.visit(sce, op.getSubOp());
        sce.getTriples().forEach(
                t -> triples2Source.put(t, op.getService())
        );
        sce.getFilters().forEach(
                f -> filters.add(f)
        );
    }

    public void visit(OpFilter op) {
        filters.addAll(op.getExprs().getList());
        OpVisitorRouter.visit(this, op.getSubOp());
    }
}
