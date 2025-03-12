package se.liu.ida.hefquin.engine;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.expr.aggregate.AccumulatorExpr;
import org.apache.jena.sparql.expr.aggregate.AccumulatorFactory;
import org.apache.jena.sparql.function.FunctionEnv;

import static se.liu.ida.hefquin.jenaintegration.sparql.FrawConstants.MAPPING_PROBABILITY;

public class RawAverageAggregator {

    public static AccumulatorFactory factory() {
        return (agg, distinct) -> new RawAverageAccumulator(agg.getExpr(), distinct);
    }

    @Override
    public int hashCode() {
        return 0;
    }

    private static class RawAverageAccumulator extends AccumulatorExpr {

        private Double numberOfWalks = 0.0;
        private Double totalCountEstimation = 0.0;
        private Double totalSumEstimation = 0.0;

        public RawAverageAccumulator(Expr expr, boolean makeDistinct) {
            super(expr, false);
//            super(expr, makeDistinct);
        }

        @Override
        public void accumulate(NodeValue nv, Binding binding, FunctionEnv functionEnv) {
            Node n = binding.get(MAPPING_PROBABILITY);
            if (n != null && n.isLiteral()) {
                Double probability = Double.valueOf(n.getLiteralValue().toString());
                Double countEstimation = 1.0 / probability;
                totalCountEstimation += countEstimation;

                if(nv != null && nv.isLiteral() && nv.isDouble()) {
                    Double value = nv.getDouble();
                    Double sumEstimation = value * countEstimation;
                    totalSumEstimation += sumEstimation;
                }
            }

            numberOfWalks += 1.0;
        }

        @Override
        public NodeValue getValue() {
            Double finalCountEstimation = totalCountEstimation / numberOfWalks;
            Double averageEstimation = totalSumEstimation / finalCountEstimation;
            return NodeValue.makeDouble(averageEstimation);
        }

        @Override
        protected NodeValue getAccValue() {
            Double finalCountEstimation = totalCountEstimation / numberOfWalks;
            Double averageEstimation = totalSumEstimation / finalCountEstimation;
            return NodeValue.makeDouble(averageEstimation);
        }

        @Override
        protected void accumulateError(Binding binding, FunctionEnv functionEnv) {

        }
    }
}
