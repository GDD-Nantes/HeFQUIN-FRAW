package se.liu.ida.hefquin.engine;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.expr.aggregate.Accumulator;
import org.apache.jena.sparql.expr.aggregate.AccumulatorFactory;
import org.apache.jena.sparql.expr.aggregate.AggCustom;
import org.apache.jena.sparql.function.FunctionEnv;

import static se.liu.ida.hefquin.jenaintegration.sparql.FrawConstants.MAPPING_PROBABILITY;

public class RawCountAggregator {

    public static AccumulatorFactory factory() {
        return (agg, distinct) -> new RawCountAccumulator(agg, distinct);
    }

    private static class RawCountAccumulator implements Accumulator{

        private Double numberOfWalks = 0.0;
        private Double estimation = 0.0;

        public RawCountAccumulator(AggCustom agg, boolean distinct) {}

        @Override
        public void accumulate(Binding binding, FunctionEnv functionEnv) {
            Node n = binding.get(MAPPING_PROBABILITY);
            if (n != null && n.isLiteral()) {
                Double value = Double.valueOf(n.getLiteralValue().toString());
                Double estimate = 1.0 / value;
                estimation += estimate;
            }

            numberOfWalks += 1.0;
        }

        @Override
        public NodeValue getValue() {
            return NodeValue.makeDouble(estimation / numberOfWalks);
        }
    }
}
