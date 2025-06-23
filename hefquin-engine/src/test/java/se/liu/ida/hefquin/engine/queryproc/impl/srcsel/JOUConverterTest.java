package se.liu.ida.hefquin.engine.queryproc.impl.srcsel;

import fr.gdd.fedqpl.visitors.ReturningArgsOpVisitor;
import fr.gdd.fedqpl.visitors.ReturningArgsOpVisitorRouter;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.E_NotEquals;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprVar;
import org.apache.jena.sparql.expr.nodevalue.NodeValueNode;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class JOUConverterTest {
    String queryNominal =
            "SELECT * WHERE { { SERVICE <http://example1.com> { ?s ?p ?o. ?o ?p1 ?o1. } } UNION { SERVICE <http://example2.com> { ?s ?p ?o. ?o ?p1 ?o1. } } }";


    Triple tp1 = Triple.create(
            NodeFactory.createVariable("s"),
            NodeFactory.createVariable("p"),
            NodeFactory.createVariable("o")
    );

    Triple tp2 = Triple.create(
            NodeFactory.createVariable("o"),
            NodeFactory.createVariable("p1"),
            NodeFactory.createVariable("o1")
    );

    Triple tp3 = Triple.create(
            NodeFactory.createVariable("a"),
            NodeFactory.createVariable("b"),
            NodeFactory.createVariable("c")
    );

    Op opTriple1 = new OpTriple(tp1);
    Op opTriple2 = new OpTriple(tp2);
    Op opTriple3 = new OpTriple(tp3);

    Node service1 = NodeFactory.createURI("http://example1.com");
    Node service2 = NodeFactory.createURI("http://example2.com");
    Node service3 = NodeFactory.createURI("http://example3.com");

    Op actualNominal = OpUnion.create(
            new OpService(service1, new OpBGP(BasicPattern.wrap(List.of(tp1, tp2))), false),
            new OpService(service2, new OpBGP(BasicPattern.wrap(List.of(tp1, tp2))), false)
    );

    Op expectedNominal = OpJoin.create(
            OpUnion.create(
                    new OpService(service1, opTriple1, false),
                    new OpService(service2, opTriple1, false)
            ),
            OpUnion.create(
                    new OpService(service1, opTriple2, false),
                    new OpService(service2, opTriple2, false)
            )
    );

    @Test
    public void testNominalCase(){

        Op actual = new JOUConverter().convert(actualNominal);
        Assert.assertTrue(areEqual(actual, expectedNominal));
        assertNominalProperJoinOverUnion(actual);
    }
//
//    String queryFilter =
//            "SELECT * WHERE { { SERVICE <http://example1.com> { ?s ?p ?o. ?o ?p1 ?o1. FILTER (?s != 'A') } } UNION { SERVICE <http://example2.com> { ?s ?p ?o. ?o ?p1 ?o1. FILTER (?s != 'A') } } }";
//
//    String expectedFilter =
//            "SELECT * WHERE {" +
//                    "   { SERVICE <http://example1.com> { ?s ?p ?o. } } UNION { SERVICE <http://example2.com> { ?s ?p ?o. } }." +
//                    "   { SERVICE <http://example1.com> { ?o ?p1 ?o1. } } UNION { SERVICE <http://example2.com> { ?o ?p1 ?o1.} }." +
//                    "   FILTER (?s != 'A')" +
//                    "}";

    Expr filterExpr = new E_NotEquals(
            new ExprVar("s"),
            NodeValueNode.makeString("A")
    );

    Op actualFilter = OpUnion.create(
            new OpService(
                    service1,
                    OpFilter.filter(
                            filterExpr,
                            new OpBGP(BasicPattern.wrap(List.of(tp1, tp2)))), false),
            new OpService(
                    service2,
                    OpFilter.filter(
                            filterExpr,
                            new OpBGP(BasicPattern.wrap(List.of(tp1, tp2)))), false));

    Op expectedFilter = OpFilter.filter(
                    filterExpr,
                    OpJoin.create(
                            OpUnion.create(
                                    new OpService(service1, opTriple1, false),
                                    new OpService(service2, opTriple1, false)),
                            OpUnion.create(
                                    new OpService(service1, opTriple2, false),
                                    new OpService(service2, opTriple2, false))));

    @Test
    public void testFilter(){
        Op actual = new JOUConverter().convert(actualFilter);
        Assert.assertTrue(actual instanceof OpFilter);
        Assert.assertEquals(((OpFilter) actual).getExprs().size(), 1);
        Assert.assertEquals(((OpFilter) actual).getExprs().get(0), filterExpr);

        Assert.assertTrue(areEqual(actual, expectedFilter));
    }

//    String queryUnion =
//            "SELECT * WHERE { { { SERVICE <http://example1.com> { ?s ?p ?o. ?o ?p1 ?o1. } } UNION { SERVICE <http://example2.com> { ?s ?p ?o. ?o ?p1 ?o1. } } } UNION { SERVICE <http://example3.com> { ?s ?p ?o. } } }";
//
//    String expectedUnion =
//            "SELECT * WHERE {" +
//                    "   { { SERVICE <http://example1.com> { ?s ?p ?o. } } UNION { SERVICE <http://example2.com> { ?s ?p ?o. } }." +
//                    "   { SERVICE <http://example1.com> { ?o ?p1 ?o1. } } UNION { SERVICE <http://example2.com> { ?o ?p1 ?o1. } } }" +
//                    "   UNION " +
//                    "   { SERVICE <http://example3.com> { ?s ?p ?o } }" +
//                    "}";

    Op actualNaturalUnion = OpUnion.create(
            OpUnion.create(
                    new OpService(service1, new OpBGP(BasicPattern.wrap(List.of(tp1, tp2))), false),
                    new OpService(service2, new OpBGP(BasicPattern.wrap(List.of(tp1, tp2))), false)),
            new OpService(service3, opTriple3, false));

    Op expectedNaturalUnion = OpUnion.create(
            OpJoin.create(
                    OpUnion.create(
                            new OpService(service1, opTriple1, false),
                            new OpService(service2, opTriple1, false)
                    ),
                    OpUnion.create(
                            new OpService(service1, opTriple2, false),
                            new OpService(service2, opTriple2, false)
                    )
            ),
            new OpService(service3, opTriple3, false));


    @Test
    public void testNaturalUnion(){
        Op actual = new JOUConverter().convert(actualNaturalUnion);

        Assert.assertTrue(areEqual(actual, expectedNaturalUnion));
    }

    String expectedUnionDifferentService =
            "SELECT * WHERE {" +
                    "   { { SERVICE <http://example1.com> { ?s ?p ?o. } } UNION { SERVICE <http://example2.com> { ?s ?p ?o. } }." +
                    "   { SERVICE <http://example1.com> { ?o ?p1 ?o1. } } UNION { SERVICE <http://example2.com> { ?o ?p1 ?o1. } } }" +
                    "   UNION " +
                    "   { SERVICE <http://example4.com> { ?s ?p ?o } }" +
                    "}";

    @Test
    public void testNaturalUnionDifferentService(){
//        Op actual = new JOUConverter().convert(Algebra.compile(QueryFactory.create(expectedUnionDifferentService)));
//
//        Op expected = Algebra.compile(QueryFactory.create(expectedUnion));
//
//        Assert.assertFalse(areEqual(expected, actual));

//        Op actual = new JOUConverter().convert(actualNaturalUnion);
    }

//    String queryProject =
//            "SELECT ?s ?o1 WHERE { { SERVICE <http://example1.com> { ?s ?p ?o. ?o ?p1 ?o1. } } UNION { SERVICE <http://example2.com> { ?s ?p ?o. ?o ?p1 ?o1. } } }";
//
//    String expectedProject =
//            "SELECT ?s ?o1 WHERE {" +
//                    "   { SERVICE <http://example1.com> { ?s ?p ?o. } } UNION { SERVICE <http://example2.com> { ?s ?p ?o. } }." +
//                    "   { SERVICE <http://example1.com> { ?o ?p1 ?o1.} } UNION { SERVICE <http://example2.com> { ?o ?p1 ?o1.} }." +
//                    "}";

    Op actualProject =
            new OpProject(
                    actualNominal,
                    List.of(Var.alloc("s"), Var.alloc("o1"))
            );

    Op expectedProject =
            new OpProject(
                    expectedNominal,
                    List.of(Var.alloc("s"), Var.alloc("o1"))
            );

    @Test
    public void testProject(){
        Op actual = new JOUConverter().convert(actualProject);
        Assert.assertTrue(areEqual(actual, expectedProject));
    }

    @Test
    public void test(){
    }

    // returns true if two ops are equal, except for the value of the service nodes (same triples, joins, etc...)
    public static boolean areEqual(Op op1, Op op2) {
        return ReturningArgsOpVisitorRouter.visit(new TestEqualOp(), op1, op2);
    }

    public static class TestEqualOp extends ReturningArgsOpVisitor<Boolean, Op> {

        public Boolean visit(OpProject op, Op other) {
            if( ! (other instanceof OpProject) ) {
                return false;
            }

            if( ! ( op.getVars().equals(((OpProject)other).getVars()) ) ) return false;

            return ReturningArgsOpVisitorRouter.visit(this, op.getSubOp(), ((OpProject) other).getSubOp());
        }

        public Boolean visit(OpJoin op, Op other) {
            if( ! (other instanceof OpJoin) ) {
                return false;
            }

            return ReturningArgsOpVisitorRouter.visit(this, op.getLeft(), ((OpJoin) other).getLeft())
                    && ReturningArgsOpVisitorRouter.visit(this, op.getRight(), ((OpJoin) other).getRight())
                    ||
                    ReturningArgsOpVisitorRouter.visit(this, op.getRight(), ((OpJoin) other).getLeft())
                    && ReturningArgsOpVisitorRouter.visit(this, op.getLeft(), ((OpJoin) other).getRight()) ;
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
                    && ReturningArgsOpVisitorRouter.visit(this, op.getRight(), ((OpUnion) other).getRight())
                    ||
                    ReturningArgsOpVisitorRouter.visit(this, op.getRight(), ((OpUnion) other).getLeft())
                            && ReturningArgsOpVisitorRouter.visit(this, op.getLeft(), ((OpUnion) other).getRight()) ;
        }

        public Boolean visit(OpBGP op, Op other) {
            if( other instanceof OpTriple) return ((OpTriple) other).equivalent(op);

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

        public Boolean visit(OpFilter op, Op other) {
            if( ! (other instanceof OpFilter) ) {
                return false;
            }

            OpFilter otherFilter = (OpFilter) other;

            if ( ! op.getExprs().equals( otherFilter.getExprs() ) ) return false;

            return ReturningArgsOpVisitorRouter.visit(this, op.getSubOp(), ((Op1) other).getSubOp());
        }
    }

    private static boolean assertNominalProperJoinOverUnion(Op nominal){
        Assert.assertTrue(((OpJoin) nominal).getLeft() instanceof OpUnion);
        Assert.assertTrue(((OpJoin) nominal).getRight() instanceof OpUnion);

        OpUnion left = (OpUnion) ((OpJoin) nominal).getLeft();
        OpUnion right = (OpUnion) ((OpJoin) nominal).getRight();

        Assert.assertTrue(left.getLeft() instanceof OpService);
        Assert.assertTrue(left.getRight() instanceof OpService);

        OpService leftLeft = (OpService) left.getLeft();
        OpService leftRight = (OpService) left.getRight();

        Assert.assertTrue(leftLeft.getSubOp() instanceof OpTriple);
        Assert.assertTrue(leftRight.getSubOp() instanceof OpTriple);

        Assert.assertEquals(leftLeft.getSubOp(), leftRight.getSubOp());

        Assert.assertTrue(right.getLeft() instanceof OpService);
        Assert.assertTrue(right.getRight() instanceof OpService);

        OpService rightLeft = (OpService) right.getLeft();
        OpService rightRight = (OpService) right.getRight();

        Assert.assertTrue(rightLeft.getSubOp() instanceof OpTriple);
        Assert.assertTrue(rightRight.getSubOp() instanceof OpTriple);

        Assert.assertEquals(rightLeft.getSubOp(), rightRight.getSubOp());

        return true;
    }

}
