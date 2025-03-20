package se.liu.ida.hefquin.engine;

import jakarta.json.JsonObject;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.algebra.op.OpGroup;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.expr.aggregate.Accumulator;
import org.apache.jena.sparql.expr.aggregate.AggCustom;
import org.apache.jena.sparql.util.Context;
import org.junit.Assert;
import org.junit.Test;
import se.liu.ida.hefquin.base.utils.Pair;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.FrawUtils;

import java.util.ArrayList;
import java.util.List;

import static se.liu.ida.hefquin.jenaintegration.sparql.FrawConstants.*;

public class RawCountAggregatorTest {

    // Arbitrary
    // TODO : pick a correct one
    private static final Double deltaPercentage = 5.0;

    private AggCustom createAggCustom(){
        return new AggCustom("http://customAgg/rawcount", false, new ExprList());
    }

    private Context createContext(){
        Context cxt = new Context();
        VarExprList varExprList = new VarExprList();
        cxt.set(CURRENT_OP_GROUP, new OpGroup(null, varExprList, List.of()));
        return cxt;
    }

    private Context createContext(String... groupedVars){
        Context cxt = new Context();

        List<Var> vars = new ArrayList<Var>();
        for(String groupedVar : groupedVars){
            vars.add(Var.alloc(groupedVar));
        }

        VarExprList varExprList = new VarExprList(vars);
        cxt.set(CURRENT_OP_GROUP, new OpGroup(null, varExprList, List.of()));
        return cxt;
    }

    private Binding buildBindingWithProbabilityAndValue(Double proba, String value){
        BindingBuilder bb = BindingBuilder.create();
        bb.add(MAPPING_PROBABILITY, NodeFactory.createLiteral(String.valueOf(proba)));
        bb.add(Var.alloc("value"), NodeFactory.createLiteral(value));
        return bb.build();
    }

    private Binding buildBindingWithProbabilityAndValues(Double proba, String... values){
        BindingBuilder bb = BindingBuilder.create();
        bb.add(MAPPING_PROBABILITY, NodeFactory.createLiteral(String.valueOf(proba)));
        int i = 0;
        for(String value : values){
            i++;
            bb.add(Var.alloc("value" + i), NodeFactory.createLiteral(value));
        }
        return bb.build();
    }

    private Binding buildBindingWithValues(Pair<String, String>... keyValPairs){
        BindingBuilder bb = BindingBuilder.create();
        int i = 0;
        for(Pair<String, String> kvp : keyValPairs){
            i++;
            bb.add(Var.alloc(kvp.object1), NodeFactory.createLiteral(kvp.object2));
        }
        return bb.build();
    }

    @Test
    public void testAccumulateNoGroupBy() {
        Accumulator rawAcc = RawCountAggregator.factory(createContext()).createAccumulator(createAggCustom(), false);

        Binding b1 = buildBindingWithProbabilityAndValue(0.2, "value1");
        Binding b2 = buildBindingWithProbabilityAndValue(0.4, "value2");

        rawAcc.accumulate(b1, null);
        rawAcc.accumulate(b2, null);

        NodeValue expected = NodeValue.makeDouble(3.75);

        Assert.assertEquals(expected, rawAcc.getValue());

    }

    @Test
    public void testAccumulateNoGroupByFailed() {
        Accumulator rawAcc = RawCountAggregator.factory(createContext()).createAccumulator(createAggCustom(), false);

        Binding b1 = buildBindingWithProbabilityAndValue(0.0, "value1");

        rawAcc.accumulate(b1, null);

        NodeValue expected = NodeValue.makeDouble(0.0);

        Assert.assertEquals(expected, rawAcc.getValue());

    }

    @Test
    public void testAccumulateNoGroupByFailedAndSuccess() {
        Accumulator rawAcc = RawCountAggregator.factory(createContext()).createAccumulator(createAggCustom(), false);

        Binding b1 = buildBindingWithProbabilityAndValue(0.0, "value1");
        Binding b2 = buildBindingWithProbabilityAndValue(0.5, "value1");


        rawAcc.accumulate(b1, null);
        rawAcc.accumulate(b2, null);

        NodeValue expected = NodeValue.makeDouble(1.0);

        Assert.assertEquals(expected, rawAcc.getValue());

    }

    @Test
    public void testAccumulateNoGroupByNoAccumulate() {
        Accumulator rawAcc = RawCountAggregator.factory(createContext()).createAccumulator(createAggCustom(), false);

        NodeValue expected = NodeValue.makeDouble(0.0);

        Assert.assertEquals(expected, rawAcc.getValue());
    }

    private JsonObject buildJsonOfJoinedBindings(Pair<Binding, Double>... bindingsWithProbabilities){
        JsonObject current = FrawUtils.buildScan(bindingsWithProbabilities[0].object1, bindingsWithProbabilities[0].object2);

        for(Pair<Binding, Double> pair : bindingsWithProbabilities){
            current = FrawUtils.buildJoin(current, FrawUtils.buildScan(pair.object1, pair.object2));

        }

        return current;
    }

    @Test
    public void testAccumulateGroupBy() {
        Binding b1 = buildBindingWithValues(Pair.of("key", "key"), Pair.of("val1", "1"));
        Binding b2 = buildBindingWithValues(Pair.of("notkey", "notkey"), Pair.of("val1", "1"));

        Binding m1 = FrawUtils.merge(b1, b2);

        JsonObject join = buildJsonOfJoinedBindings(Pair.of(b1, 0.1), Pair.of(b2, 0.1));

        BindingBuilder bb1 = BindingBuilder.create(m1);
        bb1.add(RANDOM_WALK_HOLDER, NodeFactory.createLiteral(join.toString()));
        Binding acc1 = bb1.build();

        Accumulator rawAcc = RawCountAggregator.factory(createContext("key")).createAccumulator(createAggCustom(), false);

        rawAcc.accumulate(acc1, null);

        // Expected is 10, because we don't take the probability of the triple where they was bound
        Double expected = NodeValue.makeDouble(10.0).getDouble();

        Assert.assertEquals(expected, rawAcc.getValue().getDouble(), deltaPercentage * expected);
    }

    @Test
    public void testAccumulateGroupByFailed() {
        Binding b1 = buildBindingWithValues(Pair.of("key", "key"), Pair.of("val1", "1"));
        Binding b2 = buildBindingWithValues(Pair.of("notkey", "notkey"), Pair.of("val1", "1"));

        Binding m1 = FrawUtils.merge(b1, b2);

        JsonObject join = buildJsonOfJoinedBindings(Pair.of(b1, 0.1), Pair.of(b2, 0.1));

        BindingBuilder bb1 = BindingBuilder.create(m1);
        bb1.add(RANDOM_WALK_HOLDER, NodeFactory.createLiteral(join.toString()));
        bb1.add(MAPPING_PROBABILITY, NodeFactory.createLiteral(String.valueOf(0.0), XSDDatatype.XSDdouble));
        Binding acc1 = bb1.build();

        Accumulator rawAcc = RawCountAggregator.factory(createContext("key")).createAccumulator(createAggCustom(), false);

        rawAcc.accumulate(acc1, null);

        Double expected = NodeValue.makeDouble(0.0).getDouble();

        Assert.assertEquals(expected, rawAcc.getValue().getDouble(), deltaPercentage * expected);
    }
}
