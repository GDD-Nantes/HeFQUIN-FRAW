package se.liu.ida.hefquin.engine.queryplan.executable.impl.ops;

import se.liu.ida.hefquin.base.data.SolutionMapping;
import se.liu.ida.hefquin.base.data.utils.FrawUtils;
import se.liu.ida.hefquin.base.query.BGP;
import se.liu.ida.hefquin.base.query.SPARQLGraphPattern;
import se.liu.ida.hefquin.base.query.TriplePattern;
import se.liu.ida.hefquin.engine.federation.SPARQLEndpoint;
import se.liu.ida.hefquin.engine.queryplan.executable.ExecOpExecutionException;
import se.liu.ida.hefquin.engine.queryplan.executable.IntermediateResultBlock;
import se.liu.ida.hefquin.engine.queryplan.executable.IntermediateResultElementSink;
import se.liu.ida.hefquin.engine.queryplan.executable.NullaryExecutableOp;
import se.liu.ida.hefquin.engine.queryproc.ExecutionContext;

import java.util.List;

public class ExecOpFrawBindJoinSPARQL extends BaseForFrawExecOpBindJoinWithRequestOps {

    public ExecOpFrawBindJoinSPARQL(TriplePattern query, SPARQLEndpoint fm, boolean useOuterJoinSemantics, boolean collectExceptions) {
        super(query, fm, useOuterJoinSemantics, collectExceptions);
    }

    public ExecOpFrawBindJoinSPARQL(SPARQLGraphPattern query, SPARQLEndpoint fm, boolean useOuterJoinSemantics, boolean collectExceptions) {
        super(query, fm, useOuterJoinSemantics, collectExceptions);
    }

    public ExecOpFrawBindJoinSPARQL(BGP query, SPARQLEndpoint fm, boolean useOuterJoinSemantics, boolean collectExceptions) {
        super(query, fm, useOuterJoinSemantics, collectExceptions);
    }
}
