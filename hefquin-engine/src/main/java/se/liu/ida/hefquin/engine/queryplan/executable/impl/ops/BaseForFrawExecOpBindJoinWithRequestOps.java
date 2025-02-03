package se.liu.ida.hefquin.engine.queryplan.executable.impl.ops;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Table;
import org.apache.jena.sparql.algebra.op.OpSequence;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.algebra.table.TableData;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import se.liu.ida.hefquin.base.data.SolutionMapping;
import se.liu.ida.hefquin.base.data.utils.SolutionMappingUtils;
import se.liu.ida.hefquin.base.query.BGP;
import se.liu.ida.hefquin.base.query.SPARQLGraphPattern;
import se.liu.ida.hefquin.base.query.TriplePattern;
import se.liu.ida.hefquin.base.query.impl.GenericSPARQLGraphPatternImpl2;
import se.liu.ida.hefquin.base.query.impl.QueryPatternUtils;
import se.liu.ida.hefquin.engine.federation.SPARQLEndpoint;
import se.liu.ida.hefquin.engine.federation.access.SPARQLRequest;
import se.liu.ida.hefquin.engine.federation.access.impl.req.SPARQLRequestImpl;
import se.liu.ida.hefquin.engine.queryplan.executable.ExecOpExecutionException;
import se.liu.ida.hefquin.engine.queryplan.executable.IntermediateResultElementSink;
import se.liu.ida.hefquin.engine.queryplan.executable.NullaryExecutableOp;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.FrawUtils;
import se.liu.ida.hefquin.engine.queryproc.ExecutionContext;

import java.util.*;

public class BaseForFrawExecOpBindJoinWithRequestOps extends BaseForExecOpBindJoinSPARQL{


    public BaseForFrawExecOpBindJoinWithRequestOps(TriplePattern query, SPARQLEndpoint fm, boolean useOuterJoinSemantics, boolean collectExceptions) {
        super(query, fm, useOuterJoinSemantics, collectExceptions);
    }

    public BaseForFrawExecOpBindJoinWithRequestOps(SPARQLGraphPattern query, SPARQLEndpoint fm, boolean useOuterJoinSemantics, boolean collectExceptions) {
        super(query, fm, useOuterJoinSemantics, collectExceptions);
    }

    public BaseForFrawExecOpBindJoinWithRequestOps(BGP query, SPARQLEndpoint fm, boolean useOuterJoinSemantics, boolean collectExceptions) {
        super(query, fm, useOuterJoinSemantics, collectExceptions);
    }

    @Override
    protected void _processWithoutSplittingInputFirst( final List<SolutionMapping> joinableInputSMs,
                                                       final IntermediateResultElementSink sink,
                                                       final ExecutionContext execCxt ) throws ExecOpExecutionException
    {
        final NullaryExecutableOp reqOp = createExecutableReqOp(joinableInputSMs);

        if ( reqOp != null ) {
            numberOfRequestOpsUsed++;

            final MyIntermediateResultElementSink mySink;
            if ( useOuterJoinSemantics )
                mySink = new MyFrawIntermediateResultElementSinkOuterJoin(sink, joinableInputSMs);
            else
                mySink = new MyFrawIntermediateResultElementSink(sink, joinableInputSMs);

            try {
                reqOp.execute(mySink, execCxt);
            }
            catch ( final ExecOpExecutionException e ) {
                final boolean requestBlockSizeReduced = reduceRequestBlockSize();
                if ( requestBlockSizeReduced && ! mySink.hasObtainedInputAlready() ) {
                    // If the request operator did not yet sent any solution
                    // mapping to the sink, then we can retry to process the
                    // given list of input solution mappings with the reduced
                    // request block size.
                    _process(joinableInputSMs, mySink, execCxt);
                }
                else {
                    throw new ExecOpExecutionException("Executing a request operator used by this bind join caused an exception.", e, this);
                }
            }

            mySink.flush();

            statsOfLastReqOp = reqOp.getStats();
            if ( statsOfFirstReqOp == null ) statsOfFirstReqOp = statsOfLastReqOp;
        }
    }

    @Override
    protected NullaryExecutableOp createExecutableReqOp(Iterable<SolutionMapping> solMaps) {
        return createExecutableRequestOperator(solMaps);
    }

    protected NullaryExecutableOp createExecutableRequestOperator(Iterable<SolutionMapping> solMaps) {

        final Set<Binding> bindings = new HashSet<>();
        final Set<Var> joinVars = new HashSet<>();

        boolean noJoinVars = false;

        for ( final SolutionMapping s : solMaps ) {

            final Binding b = SolutionMappingUtils.restrict( s.asJenaBinding(), varsInSubQuery);

            // If there exists a solution mapping that does not have any variables in common with the triple pattern of this operator
            // retrieve all matching triples of the given query
            if ( b.isEmpty() ) {
                noJoinVars = true;
                break;
            }

            if ( ! SolutionMappingUtils.containsBlankNodes(b) ) {
                bindings.add(b);

                final Iterator<Var> it = b.vars();
                while ( it.hasNext() ) {
                    joinVars.add( it.next() );
                }
            }
        }

        if (noJoinVars) {
            return new ExecOpRequestSPARQL( new SPARQLRequestImpl(query), fm, false );
        }

        if ( bindings.isEmpty() ) {
            return null;
        }

        final Table table = new TableData( new ArrayList<>(joinVars), new ArrayList<>(bindings) );
        final Op op = OpSequence.create( OpTable.create(table), QueryPatternUtils.convertToJenaOp(query) );
        final SPARQLGraphPattern pattern = new GenericSPARQLGraphPatternImpl2(op);
        final SPARQLRequest request = new SPARQLRequestImpl(pattern);
        return new ExecOpRequestSPARQL(request, fm, false);
    }


    protected class MyFrawIntermediateResultElementSink extends MyIntermediateResultElementSink implements IntermediateResultElementSink {

        public MyFrawIntermediateResultElementSink(IntermediateResultElementSink outputSink, Iterable<SolutionMapping> inputSolutionMappings) {
            super(outputSink, inputSolutionMappings);
        }

        @Override
        protected void _send( final SolutionMapping smFromRequest ) {
            // TODO: this implementation is very inefficient
            // We need an implementation of inputSolutionMappings that can
            // be used like an index.
            // See: https://github.com/LiUSemWeb/HeFQUIN/issues/3
            for ( final SolutionMapping smFromInput : inputSolutionMappings ) {
                if ( FrawUtils.compatible(smFromInput, smFromRequest) ) {
                    numberOfOutputMappingsProduced++;
                    outputSink.send( FrawUtils.merge(smFromInput,smFromRequest) );
                }
            }
        }
    }

    protected class MyFrawIntermediateResultElementSinkOuterJoin extends MyIntermediateResultElementSinkOuterJoin {

        public MyFrawIntermediateResultElementSinkOuterJoin(IntermediateResultElementSink outputSink, Iterable<SolutionMapping> inputSolutionMappings) {
            super(outputSink, inputSolutionMappings);
        }

        @Override
        public void _send( final SolutionMapping smFromRequest ) {
            // TODO: this implementation is very inefficient
            // We need an implementation of inputSolutionMappings that can
            // be used like an index.
            // See: https://github.com/LiUSemWeb/HeFQUIN/issues/3
            for ( final SolutionMapping smFromInput : inputSolutionMappings ) {
                if ( FrawUtils.compatible(smFromInput, smFromRequest) ) {
                    numberOfOutputMappingsProduced++;
                    outputSink.send( FrawUtils.merge(smFromInput,smFromRequest) );
                    inputSolutionMappingsWithJoinPartners.add(smFromInput);
                }
            }
        }
    }
}
