package se.liu.ida.hefquin.engine;

import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.expr.aggregate.Accumulator;

public interface ContextDependentAccumulator extends Accumulator {
    // Interface mainly for testing purposes

    public Double getFinalEstimationWithfactor(Double factor);
    public NodeValue getValueWithFactor(Double factor);
}
