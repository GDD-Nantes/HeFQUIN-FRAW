package se.liu.ida.hefquin.engine.queryproc.impl.srcsel;

import fr.gdd.fedup.FedUP;
import fr.gdd.fedup.summary.ModuloOnSuffix;
import fr.gdd.fedup.summary.Summary;
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpProject;
import se.liu.ida.hefquin.base.utils.Pair;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlan;
import se.liu.ida.hefquin.engine.queryproc.QueryProcContext;
import se.liu.ida.hefquin.engine.queryproc.SourcePlanningException;
import se.liu.ida.hefquin.engine.queryproc.SourcePlanningStats;

import java.nio.file.Path;
import java.util.Objects;
import java.util.function.Function;

public class FedupBasedJOUSourcePlannerImpl extends ServiceClauseBasedSourcePlannerImpl{
    FedUP fedup;

    public FedupBasedJOUSourcePlannerImpl(QueryProcContext qpc, String summaries, String lambdaString) {
        super(qpc);

        Summary summary = new Summary(new ModuloOnSuffix(1), Location.create(Path.of(summaries)));
        fedup = new FedUP(summary);

        Function<String, String> lambda =
                InMemoryLambdaJavaFileObject.getLambda("ModifyEndpoints",
                        lambdaString, "String");
        if (Objects.isNull(lambda)) {
            throw new UnsupportedOperationException("The lambda expression does not seem valid.");
        }
        fedup.modifyEndpoints(lambda);
    }

    @Override
    protected Pair<LogicalPlan, SourcePlanningStats> createSourceAssignment(Op jenaOp) throws SourcePlanningException {
        Op op = fedup.queryJenaToJena(jenaOp);

        final LogicalPlan sa = createPlan(op);

        final LogicalPlan jou = JOUConverterUtils.unionOverJoin2JoinOverUnion(sa);

        final SourcePlanningStats myStats = new SourcePlanningStatsImpl();

        return new Pair<>(jou, myStats);
    }

    protected LogicalPlan createPlan(Op op) {
        if(op instanceof OpProject){
            return super.createPlan(((OpProject) op).getSubOp());
        }
        return super.createPlan(op);
    }

//    static public class TripleToUnionOfTriplesVisitor {
//
//        MultiValuedMap<TriplePattern, FederationMember> tpsl;
//
//        public TripleToUnionOfTriplesVisitor(MultiValuedMap<TriplePattern, FederationMember> tpsl) {
//            this.tpsl = tpsl;
//        }
//
//        public void visit( final LogicalPlan lp){
//            if( lp instanceof LogicalPlanWithNullaryRootImpl){
//                this.visit((LogicalPlanWithNullaryRootImpl) lp);
//            }
//            if( lp instanceof LogicalPlanWithUnaryRootImpl){
//                this.visit( (LogicalPlanWithUnaryRootImpl) lp );
//            }
//            if( lp instanceof LogicalPlanWithBinaryRoot){
//                this.visit((LogicalPlanWithBinaryRoot) lp);
//            }
//            if( lp instanceof LogicalPlanWithNaryRootImpl){
//                this.visit((LogicalPlanWithNaryRootImpl) lp);
//            }
//        }
//
//        public void visit( final LogicalPlanWithNullaryRoot op ) {
//            if(op.getRootOperator() instanceof LogicalOpRequest<?,?>) {
//
//            }
//        }
//
//        public void visit( final LogicalPlanWithUnaryRoot op ) {
//            this.visit(op.getSubPlan());
//        }
//
//        public void visit( final LogicalPlanWithBinaryRoot op ) {
//            this.visit(op.getSubPlan1());
//        }
//
//        public void visit( final LogicalPlanWithNaryRoot op ) {
//            for (Iterator<LogicalPlan> it = op.getSubPlans(); it.hasNext(); ) {
//                LogicalPlan plan = it.next();
//                this.visit(plan);
//            }
//        }
//    } // end of class SourceAssignmentChecker
}
