package se.liu.ida.hefquin.engine;

import org.apache.jena.graph.Node;
import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.expr.aggregate.Accumulator;
import org.apache.jena.sparql.expr.aggregate.AccumulatorFactory;
import org.apache.jena.sparql.expr.aggregate.AggCustom;
import org.apache.jena.sparql.function.FunctionEnv;
import org.apache.jena.sparql.util.Context;

import static se.liu.ida.hefquin.jenaintegration.sparql.FrawConstants.MAPPING_PROBABILITY;


public class RawCountAggregator {

    public static AccumulatorFactory factory() {
        return (agg, distinct) -> new RawCountAccumulator(agg, distinct, ARQ.getContext());
    }

    // Mostly for testing purposes
    public static AccumulatorFactory factory(Context cxt) {
        return (agg, distinct) -> new RawCountAccumulator(agg, distinct, cxt);
    }

    public static boolean isRawCountAccumulator(Accumulator accumulator) {
        return accumulator instanceof RawCountAccumulator;
    }

    public static class RawCountAccumulator implements ContextDependentAccumulator{

        private Double numberOfWalks = 0.0;
        private Double estimation = 0.0;

        public RawCountAccumulator(AggCustom agg, boolean distinct, Context context) {}

        @Override
        public void accumulate(Binding binding, FunctionEnv functionEnv) {
            Node probabilityNode = binding.get(MAPPING_PROBABILITY);
            if (probabilityNode != null && probabilityNode.isLiteral()) {
                Double probability = Double.valueOf(probabilityNode.getLiteralValue().toString());
                if(probability == 0.0) return;
                Double estimate = 1.0 / probability;
                estimation += estimate;
            }
            numberOfWalks += 1.0;
        }

        public Double getFinalEstimation() {
            if(numberOfWalks == 0.0) return 0.0;

            return estimation/numberOfWalks;
        }

        public Double getFinalEstimationWithfactor(Double factor) {
            return getFinalEstimation() * factor;
        }

        @Override
        public NodeValue getValue() {
            return NodeValue.makeDouble(getFinalEstimation());
        }

        @Override
        public NodeValue getValueWithFactor(Double factor) {
            return NodeValue.makeDouble(getFinalEstimationWithfactor(factor));
        }
    }
}
