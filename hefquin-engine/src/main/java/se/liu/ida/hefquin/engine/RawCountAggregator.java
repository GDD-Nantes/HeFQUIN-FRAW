package se.liu.ida.hefquin.engine;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import org.apache.jena.graph.Node;
import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.algebra.op.OpGroup;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.expr.aggregate.Accumulator;
import org.apache.jena.sparql.expr.aggregate.AccumulatorFactory;
import org.apache.jena.sparql.expr.aggregate.AggCustom;
import org.apache.jena.sparql.function.FunctionEnv;
import se.liu.ida.hefquin.base.utils.Pair;
import se.liu.ida.hefquin.jenaintegration.sparql.FrawConstants;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static se.liu.ida.hefquin.engine.queryplan.executable.impl.FrawUtils.destringifyBindingJson;
import static se.liu.ida.hefquin.jenaintegration.sparql.FrawConstants.CURRENT_OP_GROUP;
import static se.liu.ida.hefquin.jenaintegration.sparql.FrawConstants.MAPPING_PROBABILITY;


public class RawCountAggregator {

    public static AccumulatorFactory factory() {
        return (agg, distinct) -> new RawCountAccumulator(agg, distinct);
    }

    private static class RawCountAccumulator implements Accumulator{

        private Double numberOfWalks = 0.0;
        private Double estimation = 0.0;
        private RandomWalkHolder randomWalkHolder;
        private List<Pair<Double, List<ProbabilityModifier>>> probaAndModifiers = new ArrayList<>();
        private Boolean isGrouped;
        private List<Var> variablesToGroup;
        private OpGroup opGroup;

        public RawCountAccumulator(AggCustom agg, boolean distinct) {
            randomWalkHolder = new RandomWalkHolder();
            opGroup = ARQ.getContext().get(CURRENT_OP_GROUP);
        }

        @Override
        public void accumulate(Binding binding, FunctionEnv functionEnv) {
            // We have to do the initialization step here, because it is the only method in which we have access to
            // functionEnv, whose context holds the group by variables
            if(Objects.isNull(variablesToGroup)) {
                List<Var> vars = opGroup.getGroupVars().getVars();
                isGrouped = Objects.nonNull(vars) && !vars.isEmpty();
                this.variablesToGroup = vars;
            }

            if(isGrouped) {
                accumulateGroupBy(binding, functionEnv);
            }else {
                accumulateNoGroupBy(binding, functionEnv);
            }

            numberOfWalks += 1.0;
        }

        private void accumulateGroupBy(Binding binding, FunctionEnv functionEnv) {
            Node rwhNode = binding.get(FrawConstants.RANDOM_WALK_HOLDER);
            String rwhString =  String.valueOf(rwhNode);
            String parsableRwhString = destringifyBindingJson(rwhString);
            JsonObject rwhJson = Json.createReader(new StringReader(parsableRwhString)).readObject();

            randomWalkHolder.initialize(rwhJson, opGroup.getGroupVars().getVars());

            List<ProbabilityModifier> probabilityModifiers = randomWalkHolder.addWalk(rwhJson);

            Double walkProbability = rwhJson.getJsonNumber("probability").doubleValue();

            probaAndModifiers.add(new Pair<>(walkProbability, probabilityModifiers));
        }

        private void accumulateNoGroupBy(Binding binding, FunctionEnv functionEnv) {
            Node n = binding.get(MAPPING_PROBABILITY);
            if (n != null && n.isLiteral()) {
                Double value = Double.valueOf(n.getLiteralValue().toString());
                Double estimate = 1.0 / value;
                estimation += estimate;
            }
        }

        @Override
        public NodeValue getValue() {


            if(numberOfWalks == 0.0) return NodeValue.makeInteger(0);

            if(isGrouped) {
                // We are in a group by scenario, so
                // we compute the estimations' sum once all the walks are done

                for(Pair<Double, List<ProbabilityModifier>> pair : probaAndModifiers) {
                    Double proba = pair.object1;
                    List<ProbabilityModifier> modifiers = pair.object2;

                    Double modifiedProbability = modifiers
                            .stream()
                            .map(modifier -> modifier.getModifier())
                            .reduce(proba, (soFar, nextModifider) -> soFar / nextModifider);

                    estimation += 1.0 / modifiedProbability;
                }

            }

            return NodeValue.makeDouble(estimation/numberOfWalks);
        }
    }
}
