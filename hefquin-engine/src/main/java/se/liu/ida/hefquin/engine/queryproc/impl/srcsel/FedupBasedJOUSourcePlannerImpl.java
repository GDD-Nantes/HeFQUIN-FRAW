package se.liu.ida.hefquin.engine.queryproc.impl.srcsel;

import fr.gdd.fedup.FedUP;
import fr.gdd.fedup.summary.ModuloOnSuffix;
import fr.gdd.fedup.summary.Summary;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprList;
import se.liu.ida.hefquin.base.query.TriplePattern;
import se.liu.ida.hefquin.base.utils.Pair;
import se.liu.ida.hefquin.engine.federation.FederationMember;
import se.liu.ida.hefquin.engine.federation.access.SPARQLRequest;
import se.liu.ida.hefquin.engine.federation.access.impl.req.SPARQLRequestImpl;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalOperator;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlan;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.*;
import se.liu.ida.hefquin.engine.queryproc.QueryProcContext;
import se.liu.ida.hefquin.engine.queryproc.SourcePlanningException;
import se.liu.ida.hefquin.engine.queryproc.SourcePlanningStats;
import se.liu.ida.hefquin.engine.queryproc.impl.loptimizer.heuristics.utils.JOUQueryAnalyzer;

import java.nio.file.Path;
import java.util.*;
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

        final LogicalPlan jou = unionOverJoin2JoinOverUnion(sa);

        final SourcePlanningStats myStats = new SourcePlanningStatsImpl();

        return new Pair<>(jou, myStats);
    }

    protected LogicalPlan createPlan(Op op) {
        if(op instanceof OpProject){
            return super.createPlan(((OpProject) op).getSubOp());
        }
        return super.createPlan(op);
    }

    private static boolean isJoinOfRequest(LogicalPlan lp){
        for(int i = 0; i < lp.numberOfSubPlans(); i++){
            if(! (lp.getSubPlan(i).getRootOperator() instanceof LogicalOpRequest<?,?>))
                return false;
        }

        return true;
    }

    private static boolean isSimpleUnionOverJoin(LogicalPlan lp){

        LogicalOperator lop = lp.getRootOperator();

        if( ! (lop instanceof LogicalOpUnion || lop instanceof LogicalOpMultiwayUnion || lop instanceof LogicalOpFilter))
            return false;

        for(int i = 0; i < lp.numberOfSubPlans(); i++){
            if(! isJoinOfRequest(lp.getSubPlan(i)))
                return false;
        }

        return true;
    }

    private static LogicalPlan simpleUnionOverJoin2JoinOverUnion(LogicalPlan lp){
        JOUQueryAnalyzer eqa = new JOUQueryAnalyzer(lp);
        MultiValuedMap<TriplePattern, FederationMember> tpsl = eqa.getTpsl();
        List<LogicalOpFilter> filters = eqa.getFilters();

        List<LogicalPlan> rootJoinChildren = new ArrayList<>();

        for (TriplePattern tp : tpsl.keySet()) {

            List<LogicalPlan> childUnionChildren = new ArrayList<>();

            for (FederationMember member : tpsl.get(tp)) {

                SPARQLRequest req = new SPARQLRequestImpl(tp);

                LogicalPlan subPlanTP = new LogicalPlanWithNullaryRootImpl(new LogicalOpRequest<>(member, req));

                childUnionChildren.add(subPlanTP);
            }

            LogicalPlan subPlanUnion =
                    new LogicalPlanWithNaryRootImpl(LogicalOpMultiwayUnion.getInstance(), childUnionChildren);

            rootJoinChildren.add(subPlanUnion);

        }

        LogicalPlan rootPlanJoin =
                new LogicalPlanWithNaryRootImpl(LogicalOpMultiwayJoin.getInstance(), rootJoinChildren);

        Set<Expr> filterExpressions = new HashSet<>();
        for(LogicalOpFilter filter : filters){
            if(filterExpressions.addAll(filter.getFilterExpressions().getList()));
        }

        if(filterExpressions.isEmpty()){
            // there are no filters in the plan, we just return the join of unions
            return rootPlanJoin;
        }

        // there are filters to apply, we create a filter operator atop the root join to create a plan that we return.
        // this is always semantically correct, and filter push down optimizations are applied later
        return new LogicalPlanWithUnaryRootImpl(new LogicalOpFilter(ExprList.create(filterExpressions)), rootPlanJoin);
    }

    protected static LogicalPlan unionOverJoin2JoinOverUnion(LogicalPlan lp){

        if ( isSimpleUnionOverJoin(lp) ) {
            return simpleUnionOverJoin2JoinOverUnion(lp);
        }

        if(lp.getRootOperator() instanceof LogicalOpMultiwayUnion){
            List<LogicalPlan> subPlans = new ArrayList<>();

            for(int i = 0; i < lp.numberOfSubPlans(); i++){
                LogicalPlan subPlan = unionOverJoin2JoinOverUnion(lp.getSubPlan(i));
                subPlans.add(subPlan);
            }

            return new LogicalPlanWithNaryRootImpl(LogicalOpMultiwayUnion.getInstance(), subPlans);
        }

        throw new IllegalArgumentException("Logical Plan " + lp + " is not a well-formed fedup union over join plan");
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
