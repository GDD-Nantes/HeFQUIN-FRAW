package se.liu.ida.hefquin.engine;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.expr.aggregate.Accumulator;
import org.apache.jena.sparql.expr.aggregate.AccumulatorFactory;
import org.apache.jena.sparql.expr.aggregate.AggCustom;
import org.apache.jena.sparql.function.FunctionEnv;
import se.liu.ida.hefquin.jenaintegration.sparql.FrawConstants;

import java.util.List;
import java.util.Objects;

import static se.liu.ida.hefquin.jenaintegration.sparql.FrawConstants.VAR_GROUP_CURRENT_STAGE;
import static se.liu.ida.hefquin.jenaintegration.sparql.FrawConstants.VAR_PROBABILITY_PREFIX;

public class RawCountAggregator {

    public static AccumulatorFactory factory() {
        return (agg, distinct) -> new RawCountAccumulator(agg, distinct);
    }

    private static class RawCountAccumulator implements Accumulator{

        private Double numberOfWalks = 0.0;
        private Double estimation = 0.0;
        private ExprList exprList;

        public RawCountAccumulator(AggCustom agg, boolean distinct) {
            exprList = agg.getExprList();
        }

        @Override
        public void accumulate(Binding binding, FunctionEnv functionEnv) {

            List<Var> variablesToGroup = functionEnv.getContext().get(VAR_GROUP_CURRENT_STAGE);
            Node totalProbaNode = binding.get(FrawConstants.MAPPING_PROBABILITY);

            Double totalProbability;

            try {
                totalProbability = Double.valueOf(totalProbaNode.getLiteralValue().toString());
            } catch (NumberFormatException e) {
                throw new RuntimeException("Could not parse total proba", e);
            }

            Double groupProbability = 1.0D;

            if(Objects.nonNull(variablesToGroup)) {
                for(Var var : variablesToGroup) {
                    Node varProbaNode = binding.get(getProbaVarFromVar(var));

                    Double varProbability;

                    try {
                        varProbability = Double.valueOf(varProbaNode.getLiteralValue().toString());
                    } catch (NumberFormatException e) {
                        throw new RuntimeException("Could not parse probability of " + varProbaNode.getName(), e);
                    }

                    groupProbability = groupProbability * varProbability;
                }
            }

            numberOfWalks += 1.0;
            // 1.0 / (groupSpecificEstimation) = 1.0 / (totalProbability / groupProbability) = (groupProbability / totalProbability)
            estimation += (groupProbability / totalProbability);
        }

        @Override
        public NodeValue getValue() {
            return numberOfWalks == 0 ? NodeValue.makeInteger(0) : NodeValue.makeDouble(estimation/numberOfWalks);
        }

        private boolean isProbaVar(Var var){
            return var.getVarName().contains(VAR_PROBABILITY_PREFIX);
        }

        private String getVarFromProbaVar(Var probaVar){
            return probaVar.getVarName().split(VAR_PROBABILITY_PREFIX)[1];
        }

        private String getProbaVarFromVar(Var var){
            return VAR_PROBABILITY_PREFIX + var.getName();
        }
    }
}
