package se.liu.ida.hefquin.jenaintegration.sparql;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.util.Symbol;
import se.liu.ida.hefquin.engine.FrawEngine;
import se.liu.ida.hefquin.engine.HeFQUINEngine;
import se.liu.ida.hefquin.engine.queryproc.QueryProcessor;
import se.liu.ida.hefquin.engine.queryproc.SamplingQueryProcessor;
import se.liu.ida.hefquin.jenaintegration.sparql.engine.main.OpExecutorFraw;
import se.liu.ida.hefquin.jenaintegration.sparql.engine.main.OpExecutorHeFQUIN;

import java.util.Map;
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

    public static final String VAR_PROVENANCE_PREFIX = "provenance_";

    public static final Symbol ENGINE = Symbol.create("engine");
    public static final Symbol ENGINE_TO_QPROC =  Symbol.create("engineToQproc");

    public static final OpExecutorFactory factory = execCxt -> {
        HeFQUINEngine engine = execCxt.getContext().get(FrawConstants.ENGINE);
        QueryProcessor qProc = ((Map<HeFQUINEngine, QueryProcessor>) execCxt.getContext().get(FrawConstants.ENGINE_TO_QPROC)).get(engine);
        if(engine instanceof FrawEngine) {
            return new OpExecutorFraw((SamplingQueryProcessor) qProc, execCxt, ((FrawEngine) engine).getBudget(), ((FrawEngine) engine).getSubBudget());
        }

        return new OpExecutorHeFQUIN(qProc, execCxt);
    };
}

