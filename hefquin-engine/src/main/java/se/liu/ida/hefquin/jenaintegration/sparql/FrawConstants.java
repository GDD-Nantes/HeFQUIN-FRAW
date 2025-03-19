package se.liu.ida.hefquin.jenaintegration.sparql;

import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.util.Symbol;

public class FrawConstants {
    public static final Var MAPPING_PROBABILITY           = Var.alloc("probabilityOfRetrievingRestOfMapping");

    public static final String VAR_PROBABILITY_PREFIX = "probabilityOfRetrievingVariable_";
    public static final Symbol CURRENT_OP_GROUP = Symbol.create("currentOp");
    public static final Var RANDOM_WALK_HOLDER = Var.alloc("randomWalkHolder");

}

