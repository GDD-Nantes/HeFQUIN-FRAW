package se.liu.ida.hefquin.engine.queryproc.impl.loptimizer.heuristics.utils;

import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.util.ExprUtils;
import se.liu.ida.hefquin.engine.federation.access.SPARQLEndpointInterface;
import se.liu.ida.hefquin.engine.queryplan.logical.*;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.*;

import java.util.Iterator;

public class LogicalPlan2String {

    public String printLogicalPlan(LogicalPlan logicalPlan){
        StringBuilder builder = new StringBuilder();

        builder.append("SELECT * WHERE {");

        builder.append("\n");

        builder.append(_printLogicalPlan(logicalPlan));

        builder.append("\n }");

        return builder.toString();
    }


    public String _printLogicalPlan(LogicalPlan logicalPlan){
        if(logicalPlan instanceof LogicalPlanWithUnaryRoot){
            return print((LogicalPlanWithUnaryRoot) logicalPlan);
        }

        if(logicalPlan instanceof LogicalPlanWithBinaryRoot){
            return print((LogicalPlanWithBinaryRoot) logicalPlan);
        }

        if(logicalPlan instanceof LogicalPlanWithNaryRoot){
            return print((LogicalPlanWithNaryRoot) logicalPlan);
        }

        if(logicalPlan instanceof LogicalPlanWithNullaryRoot){
            return print((LogicalPlanWithNullaryRoot) logicalPlan);
        }

        return "ERROR";
    }


    public String print(LogicalPlanWithNullaryRoot nullary){
        LogicalOperator op = nullary.getRootOperator();

        StringBuilder string = new StringBuilder();

        if(op instanceof LogicalOpRequest<?,?>){
            string.append("SERVICE SILENT <");
            string.append(((SPARQLEndpointInterface) ((LogicalOpRequest) op).getFederationMember().getInterface()).getURL());
            string.append("> ");
            string.append("{ \n");
            string.append(((LogicalOpRequest<?, ?>) op).getRequest());
            string.append("\n}");

            return string.toString();
        }

        return "NULLARY ERROR";
    }

    public String print(LogicalPlanWithUnaryRoot unary){
        LogicalOperator op = unary.getRootOperator();

        if(op instanceof LogicalOpFilter){
            StringBuilder filters = new StringBuilder();

            for (Expr expr : ((LogicalOpFilter) op).getFilterExpressions()){
                filters.append("FILTER (" + ExprUtils.fmtSPARQL(expr) + ")");
            }

            filters.append(_printLogicalPlan(unary.getSubPlan()));

            return filters.toString();
        }

        return "UNARY ERROR";
    }


    public String print(LogicalPlanWithBinaryRoot binary){
        LogicalOperator op = binary.getRootOperator();

        if(op instanceof LogicalOpUnion){
            StringBuilder union = new StringBuilder();

            union.append("{");

            union.append("\n");

            union.append(_printLogicalPlan((binary.getSubPlan1())));

            union.append("\n");

            union.append("}");

            union.append("UNION");

            union.append("{");

            union.append(_printLogicalPlan((binary.getSubPlan2())));

            union.append("\n");

            union.append("}");

            return union.toString();
        }

        return "BINARY ERROR";
    }

    public String print(LogicalPlanWithNaryRoot nary){
        LogicalOperator op = nary.getRootOperator();

        if (op instanceof LogicalOpMultiwayJoin){
            StringBuilder multiJoin = new StringBuilder();

            for (Iterator<LogicalPlan> it = nary.getSubPlans(); it.hasNext(); ) {
                LogicalPlan subPlan = it.next();

                multiJoin.append(_printLogicalPlan(subPlan));

                multiJoin.append(".");
            }

            return multiJoin.toString();
        }

        if (op instanceof LogicalOpMultiwayUnion){
            LogicalPlan nested = multiwayUnion2NestedBinaryUnion(nary.getSubPlans());

            if(nested instanceof LogicalPlanWithBinaryRoot){
                return _printLogicalPlan((LogicalPlanWithBinaryRoot) nested);
            }

            if(nested instanceof LogicalPlanWithUnaryRoot){
                return _printLogicalPlan((LogicalPlanWithUnaryRoot) nested);
            }
        }

        return "NARY ERROR";
    }


   public LogicalPlan multiwayUnion2NestedBinaryUnion(Iterator<LogicalPlan> subPlans){

       LogicalPlan first = null;
        if(subPlans.hasNext()){
            first = subPlans.next();
        } else {
            throw new IllegalArgumentException("subPlans cannot be empty; cannot create unions without operands");
        }

        if(subPlans.hasNext()){
            LogicalPlan rest = multiwayUnion2NestedBinaryUnion(subPlans);

            return new LogicalPlanWithBinaryRootImpl(LogicalOpUnion.getInstance(), first, rest);
        } else {
            return first;
        }
   }
}
