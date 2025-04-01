package se.liu.ida.hefquin.engine.queryproc.impl.srcsel;

import fr.gdd.fedqpl.visitors.ReturningArgsOpVisitor;
import fr.gdd.fedqpl.visitors.ReturningArgsOpVisitorRouter;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.junit.Assert;
import org.junit.Test;

public class JOUConverterTest {
    String queryNominal =
            "SELECT * WHERE { { SERVICE <http://example1.com> { ?s ?p ?o. ?o ?p1 ?o1. } } UNION { SERVICE <http://example2.com> { ?s ?p ?o. ?o ?p1 ?o1. } } }";

    String expectedNominal =
            "SELECT * WHERE {" +
                    "   { SERVICE <http://example1.com> { ?s ?p ?o. } } UNION { SERVICE <http://example2.com> { ?s ?p ?o. } }." +
                    "   { SERVICE <http://example1.com> { ?o ?p1 ?o1. } } UNION { SERVICE <http://example2.com> { ?o ?p1 ?o1. } }" +
                    "}";

    @Test
    public void testNominalCase(){
        Op actual = new JOUConverter().convert(Algebra.compile(QueryFactory.create(queryNominal)));

        Op expected = Algebra.compile(QueryFactory.create(expectedNominal));

        Assert.assertTrue(areEqual(expected, actual));
    }

    String queryFilter =
            "SELECT * WHERE { { SERVICE <http://example1.com> { ?s ?p ?o. ?o ?p1 ?o1. FILTER (?s != 'A') } } UNION { SERVICE <http://example2.com> { ?s ?p ?o. ?o ?p1 ?o1. FILTER (?s != 'A') } } }";

    String expectedFilter =
            "SELECT * WHERE {" +
                    "   { SERVICE <http://example1.com> { ?s ?p ?o. } } UNION { SERVICE <http://example2.com> { ?s ?p ?o. } }." +
                    "   { SERVICE <http://example1.com> { ?o ?p1 ?o1. } } UNION { SERVICE <http://example2.com> { ?o ?p1 ?o1.} }." +
                    "   FILTER (?s != 'A')" +
                    "}";

    @Test
    public void testFilter(){
        Op actual = new JOUConverter().convert(Algebra.compile(QueryFactory.create(queryFilter)));

        Op expected = Algebra.compile(QueryFactory.create(expectedFilter));

        Assert.assertTrue(areEqual(expected, actual));
    }

    String queryUnion =
            "SELECT * WHERE { { { SERVICE <http://example1.com> { ?s ?p ?o. ?o ?p1 ?o1. } } UNION { SERVICE <http://example2.com> { ?s ?p ?o. ?o ?p1 ?o1. } } } UNION { SERVICE <http://example3.com> { ?s ?p ?o. } } }";

    String expectedUnion =
            "SELECT * WHERE {" +
                    "   { { SERVICE <http://example1.com> { ?s ?p ?o. } } UNION { SERVICE <http://example2.com> { ?s ?p ?o. } }." +
                    "   { SERVICE <http://example1.com> { ?o ?p1 ?o1. } } UNION { SERVICE <http://example2.com> { ?o ?p1 ?o1. } } }" +
                    "   UNION " +
                    "   { SERVICE <http://example3.com> { ?s ?p ?o } }" +
                    "}";

    @Test
    public void testNaturalUnion(){
        Op actual = new JOUConverter().convert(Algebra.compile(QueryFactory.create(queryUnion)));

        Op expected = Algebra.compile(QueryFactory.create(expectedUnion));

        Assert.assertTrue(areEqual(expected, actual));
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
        Op actual = new JOUConverter().convert(Algebra.compile(QueryFactory.create(expectedUnionDifferentService)));

        Op expected = Algebra.compile(QueryFactory.create(expectedUnion));

        Assert.assertFalse(areEqual(expected, actual));
    }

    String queryProject =
            "SELECT ?s ?o1 WHERE { { SERVICE <http://example1.com> { ?s ?p ?o. ?o ?p1 ?o1. } } UNION { SERVICE <http://example2.com> { ?s ?p ?o. ?o ?p1 ?o1. } } }";

    String expectedProject =
            "SELECT ?s ?o1 WHERE {" +
                    "   { SERVICE <http://example1.com> { ?s ?p ?o. } } UNION { SERVICE <http://example2.com> { ?s ?p ?o. } }." +
                    "   { SERVICE <http://example1.com> { ?o ?p1 ?o1.} } UNION { SERVICE <http://example2.com> { ?o ?p1 ?o1.} }." +
                    "}";

    @Test
    public void testProject(){
        Op actual = new JOUConverter().convert(Algebra.compile(QueryFactory.create(queryProject)));

        Op expected = Algebra.compile(QueryFactory.create(expectedProject));

        Assert.assertTrue(areEqual(expected, actual));
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



}
