package se.liu.ida.hefquin.jenaintegration.sparql;

import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.util.Symbol;

public class HeFQUINConstants {
	public static final String systemVarNS = "http://ida.liu.se/HeFQUIN/system#";

	public static final Symbol sysEngine                  = Symbol.create(systemVarNS+"engine");

	public static final Symbol sysQueryProcStats          = Symbol.create(systemVarNS+"queryProcStats");
	public static final Symbol sysQueryProcExceptions     = Symbol.create(systemVarNS+"queryProcExceptions");

	public static final Var MAPPING_PROBABILITY           = Var.alloc("probabilityOfRetrievingRestOfMapping");

	public static final String VAR_PROBABILITY_PREFIX = "probabilityOfRetrievingVariable_";
	public static final Symbol VAR_GROUP_CURRENT_STAGE = Symbol.create("variablesToGroupCurrentStage");
	public static final Symbol OP_TO_GROUP_BY_VARS = Symbol.create("bgpToGroupByVariablesMap");

}
