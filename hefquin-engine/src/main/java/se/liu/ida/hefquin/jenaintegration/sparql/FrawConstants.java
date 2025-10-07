package se.liu.ida.hefquin.jenaintegration.sparql;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.util.Symbol;
import se.liu.ida.hefquin.base.data.SolutionMapping;
import se.liu.ida.hefquin.base.data.impl.SolutionMappingImpl;
import se.liu.ida.hefquin.base.data.utils.Budget;
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
    public static final Var MAPPING_PROBABILITY = Var.alloc("probabilityOfRetrievingRestOfMapping");
    public static final Symbol CURRENT_OP_GROUP = Symbol.create("currentOp");

    // QUERY SPECIFIC
    public static final Property attempts = createProperty("http://w3id.org/fraw/engineconf#attempts" );
    public static final Property remote_attempts = createProperty("http://w3id.org/fraw/engineconf#remote_attempts" );
    public static final Property limit = createProperty("http://w3id.org/fraw/engineconf#limit" );
    public static final Property timeout = createProperty("http://w3id.org/fraw/engineconf#timeout" );
    public static final Symbol ATTEMPTS = Symbol.create("attempts");
    public static final Symbol REMOTE_ATTEMPTS = Symbol.create("remote_attempts");
    public static final Symbol LIMIT = Symbol.create("limit");
    public static final Symbol TIMEOUT = Symbol.create("timeout");

    // CONFIGURATION-IMPOSED
    public static final Property max_attempts = createProperty("http://w3id.org/fraw/engineconf#max_attempts" );
    public static final Property max_remote_attempts = createProperty("http://w3id.org/fraw/engineconf#max_remote_attempts");
    public static final Property max_results = createProperty("http://w3id.org/fraw/engineconf#max_results" );
    public static final Property max_timeout = createProperty("http://w3id.org/fraw/engineconf#max_timeout" );
    public static final Symbol MAX_ATTEMPTS = Symbol.create("max_attempts");
    public static final Symbol MAX_REMOTE_ATTEMPTS = Symbol.create("max_remote_attempts");
    public static final Symbol MAX_RESULTS = Symbol.create("max_results");
    public static final Symbol MAX_TIMEOUT = Symbol.create("max_timeout");
    public static final Symbol QUERY_EXECUTION_BUDGET = Symbol.create("query_execution_budget");

    public static final Random random = new Random();

    public static final String VAR_PROVENANCE_PREFIX = "provenance_";

    public static final Symbol ENGINE = Symbol.create("engine");
    public static final Symbol ENGINE_TO_QPROC =  Symbol.create("engineToQproc");

    public static final OpExecutorFactory factory = execCxt -> {
        HeFQUINEngine engine = execCxt.getContext().get(FrawConstants.ENGINE);
        QueryProcessor qProc = ((Map<HeFQUINEngine, QueryProcessor>) execCxt.getContext().get(FrawConstants.ENGINE_TO_QPROC)).get(engine);
        if(engine instanceof FrawEngine frawEngine) {
            return new OpExecutorFraw((SamplingQueryProcessor) qProc, execCxt, execCxt.getContext().get(QUERY_EXECUTION_BUDGET));
        }

        return new OpExecutorHeFQUIN(qProc, execCxt);
    };

    public static final Budget SINGLE_WALK_BUDGET = new Budget()
            .setAttempts(1)
            .setRemoteAttempts(1)
            .setLimit(Integer.MAX_VALUE)
            .setTimeout(Integer.MAX_VALUE);

    public static final SolutionMapping empty = new SolutionMappingImpl();
}

