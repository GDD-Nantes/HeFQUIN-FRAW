package se.liu.ida.hefquin.jenaintegration.sparql;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.util.Symbol;

import java.util.Random;

import static org.apache.jena.rdf.model.ResourceFactory.createProperty;


public class FrawConstants {
    public static final Var MAPPING_PROBABILITY           = Var.alloc("probabilityOfRetrievingRestOfMapping");
    public static final Symbol CURRENT_OP_GROUP = Symbol.create("currentOp");

    public static final Property budget = createProperty( "http://w3id.org/fraw/engineconf#budget" );
    public static final Property subBudget = createProperty( "http://w3id.org/fraw/engineconf#subBudget" );
    public static final Symbol BUDGET = Symbol.create("budget");
    public static final Symbol SUB_BUDGET = Symbol.create("subBudget");

    public static final Random random = new Random();


    // not in use
    // TODO : delete eventually

    public static final Var RANDOM_WALK_HOLDER = Var.alloc("randomWalkHolder");
    public static final String VAR_PROBABILITY_PREFIX = "probabilityOfRetrievingVariable_";
}

