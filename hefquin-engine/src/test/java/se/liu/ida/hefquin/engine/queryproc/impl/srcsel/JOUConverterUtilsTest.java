package se.liu.ida.hefquin.engine.queryproc.impl.srcsel;

import org.junit.Ignore;
import org.junit.Test;
import se.liu.ida.hefquin.engine.federation.FederationMember;
import se.liu.ida.hefquin.engine.federation.catalog.impl.FederationCatalogImpl;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlan;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalOpUnion;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalPlanWithBinaryRootImpl;
import se.liu.ida.hefquin.engine.queryproc.SourcePlanningException;

import static org.junit.Assert.assertEquals;

public class JOUConverterUtilsTest extends ExhaustiveSourcePlannerImplTest{

    @Ignore
    @Test
    public void testSimpleRequestOp() throws SourcePlanningException {

        // Arrange
        final String queryString = "SELECT * WHERE {"
                + "  ?x <http://example.org/p> ?y"
                + "}";

        final FederationCatalogImpl fedCat = new FederationCatalogImpl();

        final FederationMember fm = new TPFServerForTest();
        fedCat.addMember("http://example.org", fm);

        final LogicalPlan plan = createLogicalPlan(queryString, fedCat);

        // Act
        LogicalPlan lp = JOUConverterUtils.unionOverJoin2JoinOverUnion(plan);

        // Assert
        assertEquals(lp.getRootOperator(), plan.getRootOperator());
    }

    @Ignore
    @Test
    public void testUnionOfTwoJoins() throws SourcePlanningException {
        final String queryString = "SELECT * WHERE {"
                + " ?x1 <http://example1.org/p> ?y1."
                + " ?x1 <http://example2.org/p> ?y2"
                + "}";

        final FederationCatalogImpl fedCat = new FederationCatalogImpl();

        final FederationMember fm1 = new TPFServerForTest();
        final FederationMember fm2 = new TPFServerForTest();
        fedCat.addMember("http://example1.org", fm1);
        fedCat.addMember("http://example2.org", fm2);

        final LogicalPlan join1 = createLogicalPlan(queryString, fedCat);
        final LogicalPlan join2 = createLogicalPlan(queryString, fedCat);

        LogicalPlan rootUnion = new LogicalPlanWithBinaryRootImpl(LogicalOpUnion.getInstance(), join1, join2);

        final LogicalPlan plan = createLogicalPlan(queryString, fedCat);

        LogicalPlan lp = JOUConverterUtils.unionOverJoin2JoinOverUnion(plan);
    }
}
