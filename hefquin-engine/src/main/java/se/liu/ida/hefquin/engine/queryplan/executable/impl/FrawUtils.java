package se.liu.ida.hefquin.engine.queryplan.executable.impl;

import jakarta.json.*;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import org.apache.jena.sparql.engine.binding.BindingLib;
import se.liu.ida.hefquin.base.data.SolutionMapping;
import se.liu.ida.hefquin.base.data.impl.SolutionMappingImpl;

import java.util.Iterator;

import static se.liu.ida.hefquin.jenaintegration.sparql.FrawConstants.*;

public class FrawUtils {

    public static boolean compatible(final SolutionMapping m1, final SolutionMapping m2 ) {
        final Binding b1 = m1.asJenaBinding();
        final Binding b2 = m2.asJenaBinding();

        final Iterator<Var> it = b1.vars();
        while ( it.hasNext() ) {
            final Var v = it.next();
            // TODO : make it look nicer
            if ( b2.contains(v) && ! b2.get(v).sameValueAs(b1.get(v)) && !v.equals(MAPPING_PROBABILITY))
                return false;
        }

        return true;
    }

    public static SolutionMapping merge( final SolutionMapping m1, final SolutionMapping m2 ) {
        final Binding b1 = m1.asJenaBinding();
        final Binding b2 = m2.asJenaBinding();

        return new SolutionMappingImpl( merge(b1,b2) );
    }

    public static Binding merge(Binding bind1, Binding bind2) {
        // Create binding from LHS
        BindingBuilder builder = Binding.builder(bind1);
        Iterator<Var> vIter = bind2.vars();
        boolean computedProba = false;
        // Add any variables from the RHS
        for ( ; vIter.hasNext() ; ) {
            Var v = vIter.next();
            if ( !builder.contains(v) && !isProbaVar(v))
                builder.add(v, bind2.get(v));
            else {
                // Checking!
                Node n1 = bind1.get(v);
                Node n2 = bind2.get(v);
                if(v.equals(MAPPING_PROBABILITY)){
                    if(!computedProba){
                        builder.add(v,
                                NodeFactory.createLiteralDT(String.valueOf(Double.valueOf(String.valueOf(n1.getLiteralValue())) * Double.valueOf(String.valueOf(n2.getLiteralValue()))), XSDDatatype.XSDdouble));
                    } else {
                        System.out.println("Tried to compute probability of joined binding twice! This shouldn't happen");
                    }
                    // Makes sure we only compute and add the probability once. A binding should only ever contain this variable once
                    // and we only iterate over binding2 to build the joined binding, but we still check just in case.
                    computedProba = true;
                }
                if ( !n1.equals(n2) && !v.equals(MAPPING_PROBABILITY))
                    Log.warn(BindingLib.class, "merge: Mismatch : " + n1 + " != " + n2);
            }
        }

        return builder.build();
    }

    public static Binding updateProbaUnion(SolutionMapping solutionMapping, int numberOfChildren, int chosen){
        Binding binding = solutionMapping.asJenaBinding();

        BindingBuilder bb = BindingBuilder.create();

        for (Iterator<Var> it = binding.vars(); it.hasNext(); ) {
            Var var = it.next();
            if(!var.equals(MAPPING_PROBABILITY) && !isRandomWalkHolder(var)){
                // Processing "normal" variables, aka not probabilities or whatever
                Node node = binding.get(var);
                bb.add(var, node);
            }else if(!bb.contains(MAPPING_PROBABILITY)) {
                // Processing global mapping probability
                Double newProbability = Double.valueOf(String.valueOf(binding.get(MAPPING_PROBABILITY).getLiteralValue())) / Double.valueOf(numberOfChildren);
                bb.add(MAPPING_PROBABILITY, NodeFactory.createLiteralDT(String.valueOf(newProbability), XSDDatatype.XSDdouble));
            }
            // We never copy union objects since we always create a new one right after this
        }

//        Node subUnionObjectNode = binding.get(RANDOM_WALK_HOLDER);
//        if(subUnionObjectNode == null) return bb.build();
//
//        JsonObject subUnionJson = Json.createReader(new StringReader(subUnionObjectNode.getLiteralValue().toString())).readObject();
//
//        JsonObject unionJson = buildUnion(subUnionJson, chosen);
//
//        bb.add(RANDOM_WALK_HOLDER, NodeFactory.createLiteral(unionJson.toString()));

        return bb.build();
    }

    public static boolean isProbaVar(Var var){
        return var.getVarName().contains(VAR_PROBABILITY_PREFIX);
    }

    public static String getProbaVarFromVar(Var var){
        return VAR_PROBABILITY_PREFIX + var.getName();
    }

    public static boolean isRandomWalkHolder(Var var){
        return RANDOM_WALK_HOLDER.getVarName().equals(var.getName());
    }

    public static String destringifyBindingJson(String jsonString){
        return jsonString.substring(1, jsonString.length()-1).replace("\\\"", "\"");
    }

    public static JsonObject buildUnion(JsonObject subUnionJson, int chosen){
        // Creating a new union object in the binding

        JsonArray vars = subUnionJson.getJsonArray("vars");

        Double probability = subUnionJson.getJsonNumber("probability").doubleValue();

        JsonObject currentUnionJson = Json.createObjectBuilder()
                .add("type", "union")
                .add("child", chosen)
                .add("sub", subUnionJson)
                .add("vars", vars)
                // As we retrieve a random walk, this probability cannot be known
                // in advance. It is computed based on the number of branches of this union that have been explored by
                // a number of random walks. For now, we keep the probability of the child, as if this union had only one branch,
                // and when computing the estimation later, we apply the observed probability.
                .add("probability", probability)
                .build();

        return currentUnionJson;
    }

    public static JsonObject buildScan(Binding binding, Double probability){
        JsonArrayBuilder jab = Json.createArrayBuilder();
        binding.vars().forEachRemaining(var -> jab.add(Json.createValue(var.toString())));

        JsonArray jsonArray = jab.build();

        JsonObject scanJson = Json.createObjectBuilder()
                .add("type", "scan")
                .add("vars", jsonArray)
                .add("probability", probability)
                .build();

        return scanJson;
    }

    public static JsonObject buildJoin(JsonObject left, JsonObject right){
        // Computing probabilities for current join
        JsonValue probabilityLeftJson = left.get("probability");
        Double probabilityLeft = Double.valueOf(probabilityLeftJson.toString());
        JsonValue probabilityRightJson = right.get("probability");
        Double probabilityRight = Double.valueOf(probabilityRightJson.toString());

        Double probability = probabilityLeft.doubleValue() * probabilityRight.doubleValue();

        JsonArray varsLeft = left.getJsonArray("vars");
        JsonArray varsRight = right.getJsonArray("vars");

        JsonArrayBuilder varsBuilder = Json.createArrayBuilder();
        for(JsonValue var : varsLeft){
            varsBuilder.add(var);
        }
        for(JsonValue var : varsRight){
            varsBuilder.add(var);
        }

        JsonArray vars = varsBuilder.build();

        JsonObject joinJson = Json.createObjectBuilder()
                .add("type", "join")
                .add("left", left)
                .add("right", right)
                .add("probability", probability)
                .add("vars", vars)
                .build();

        return joinJson;
    }


    /**
     * Returns <code>true</code> if the first solution mapping, b1, is
     * included in the second solution mapping, b2, where we say that
     * 'b1 is included in b2' if the variables in b1 are a proper subset
     * of the variables in b2 and the two solution mappings are compatible.
     * In other words, b1 and b2 are the same for the subset of variables
     * for which they both have bindings and, additionally, b2 has bindings
     * for additional variables.
     */
    public static boolean includedIn( final Binding b1, final Binding b2 ) {
        // First check: b1 can be included in b2 only if b1 has fewer
        // variables than b2. If that is not the case, we can immediately
        // conclude that b1 is not included in b2.
        if ( b1.size() >= b2.size() ) return false;

        // Now the main check: We iterate over the variables bound in b1 and,
        // for each of these variables, we check that
        // (1) if the variable is not the special probability variable :
        // (a) the variable is also bound in b2 and
        // (b) both solution mappings have the same term for the variable.
        final Iterator<Var> it = b1.vars();
        while ( it.hasNext() ) {
            final Var var = it.next();
            // check (1)
            if ( var.equals(MAPPING_PROBABILITY) ) continue;
            // check (a)
            if ( ! b2.contains(var) ) return false;
            // check (b)
            if ( ! b1.get(var).equals(b2.get(var)) ) return false;
        }

        return true;
    }
}
