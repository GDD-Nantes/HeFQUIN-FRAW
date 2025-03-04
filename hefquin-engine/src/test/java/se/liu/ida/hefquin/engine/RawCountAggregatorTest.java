package se.liu.ida.hefquin.engine;

import jakarta.json.JsonObject;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.expr.aggregate.Accumulator;
import org.apache.jena.sparql.function.FunctionEnv;
import org.apache.jena.sparql.function.FunctionEnvBase;
import org.apache.jena.sparql.util.Context;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import se.liu.ida.hefquin.jenaintegration.sparql.FrawConstants;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RawCountAggregatorTest {

    @Test
    public void test() {
        FunctionEnv env = createContextWithVariablesToGroup(List.of());

        assertTrue(true);
    }

    RawCountAggregator agg;
    Accumulator acc;

    @Before
    public void setUp() {
        agg = new RawCountAggregator();
        acc = agg.factory().createAccumulator(null, false);
    }

    public void testFactory_returnsNewAccumulator(){
//        Accumulator acc = agg.factory().createAccumulator(null, false);

    }

    @Ignore
    public void testGetValue_returns0_whenNoAccumulateCall(){
        Double val = acc.getValue().getDouble();
        assertEquals(val, Double.valueOf(0.0D));
    }

    @Ignore
    public void testGetValue_returns_whenAccumulateCall(){
        Var groupByVar = Var.alloc("gbv");
        createContextWithVariablesToGroup(List.of(groupByVar));


        Binding binding = BindingFactory.binding();

//        acc.accumulate();

        Double val = acc.getValue().getDouble();
    }

    private JsonObject createRandomWalkHolder(){

        return null;
    }

    private FunctionEnv createContextWithVariablesToGroup(List<Var> vars) {
        FunctionEnv env = new FunctionEnvBase();
        Context context = env.getContext();

        context.set(FrawConstants.VAR_GROUP_CURRENT_STAGE, vars);

        return env;
    }
}
