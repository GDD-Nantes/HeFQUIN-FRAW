package se.liu.ida.hefquin.engine;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import org.apache.jena.graph.Node;
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

        public RawCountAccumulator(AggCustom agg, boolean distinct) {
            randomWalkHolder = new RandomWalkHolder();
        }

        @Override
        public void accumulate(Binding binding, FunctionEnv functionEnv) {
            // We have to do the initialization step here, because it is the only method in which we have access to
            // functionEnv, whose context holds the group by variables
            if(Objects.isNull(variablesToGroup)) {
                List<Var> variablesToGroup = functionEnv.getContext().get(FrawConstants.VAR_GROUP_CURRENT_STAGE);
                isGrouped = Objects.nonNull(variablesToGroup) && !variablesToGroup.isEmpty();
                this.variablesToGroup = variablesToGroup;
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

            randomWalkHolder.initialize(rwhJson, functionEnv.getContext().get(FrawConstants.VAR_GROUP_CURRENT_STAGE));

            List<ProbabilityModifier> probabilityModifiers = randomWalkHolder.addWalk(rwhJson);

            Double walkProbability = rwhJson.getJsonNumber("probability").doubleValue();

            probaAndModifiers.add(new Pair<>(walkProbability, probabilityModifiers));
        }

        private void accumulateNoGroupBy(Binding binding, FunctionEnv functionEnv) {
            Node rwhNode = binding.get(FrawConstants.RANDOM_WALK_HOLDER);
            String rwhString =  String.valueOf(rwhNode);
            String parsableRwhString = destringifyBindingJson(rwhString);
            JsonObject rwhJson = Json.createReader(new StringReader(parsableRwhString)).readObject();

            randomWalkHolder.initialize(rwhJson, List.of());

            List<ProbabilityModifier> probabilityModifiers = randomWalkHolder.addWalk(rwhJson);

            Double walkProbability = rwhJson.getJsonNumber("probability").doubleValue();

            probaAndModifiers.add(new Pair<>(walkProbability, probabilityModifiers));
        }

//        private void accumulateNoGroupBy(Binding binding, FunctionEnv functionEnv) {
//            Node totalProbaNode = binding.get(FrawConstants.MAPPING_PROBABILITY);
//
//            if(Objects.isNull(totalProbaNode)) return;
//
//            Double probability;
//
//            try {
//                probability = Double.valueOf(totalProbaNode.getLiteralValue().toString());
//            } catch (NumberFormatException e) {
//                throw new RuntimeException("Could not parse total proba", e);
//            }
//
//            estimation += (1.0 / probability);
//        }

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

            } else {
                // We are not in a group by scenario, ie a select count query. The estimations' sum has already been
                // computed. Nothing to do
            }


            return NodeValue.makeDouble(estimation/numberOfWalks);


        }
    }
}
