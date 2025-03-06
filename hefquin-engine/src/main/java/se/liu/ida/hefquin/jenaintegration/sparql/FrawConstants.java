package se.liu.ida.hefquin.jenaintegration.sparql;

import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.util.Symbol;

public class FrawConstants {
    public static final String VAR_PROBABILITY_PREFIX = "probabilityOfRetrievingVariable_";
    public static final Symbol VAR_GROUP_CURRENT_STAGE = Symbol.create("variablesToGroupCurrentStage");
    public static final Var MAPPING_PROBABILITY           = Var.alloc("probabilityOfRetrievingRestOfMapping");
}
