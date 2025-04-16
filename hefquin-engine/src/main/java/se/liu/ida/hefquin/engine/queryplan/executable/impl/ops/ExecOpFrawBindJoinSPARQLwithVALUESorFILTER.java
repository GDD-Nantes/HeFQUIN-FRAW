package se.liu.ida.hefquin.engine.queryplan.executable.impl.ops;

import se.liu.ida.hefquin.base.query.SPARQLGraphPattern;
import se.liu.ida.hefquin.engine.federation.FederationMemberAgglomeration;
import se.liu.ida.hefquin.engine.federation.SPARQLEndpoint;
import se.liu.ida.hefquin.engine.queryplan.executable.ExecOpExecutionException;
import se.liu.ida.hefquin.engine.queryplan.executable.IntermediateResultBlock;
import se.liu.ida.hefquin.engine.queryplan.executable.IntermediateResultElementSink;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ExecutableOperatorStatsImpl;
import se.liu.ida.hefquin.engine.queryproc.ExecutionContext;

import java.util.Collections;
import java.util.List;

/**
 * Implementation of (a batching version of) the bind join algorithm that starts by
 * using a VALUES clause exactly as done by {@link ExecOpBindJoinSPARQLwithVALUES}.
 * If this fails for the first request, the implementation repeats the request by
 * using FILTERs as done by {@link ExecOpBindJoinSPARQLwithFILTER} and, then,
 * continues using the FILTER-based approach for the rest of the requests. If the
 * first VALUES-based request succeeds, however, then the implementation continues
 * using the VALUES-based approach for the rest of the requests. 
 */
public class ExecOpFrawBindJoinSPARQLwithVALUESorFILTER extends BaseForExecOpBindJoin<SPARQLGraphPattern, SPARQLEndpoint>
{
	protected final boolean useOuterJoinSemantics;

	// will be initialized when processing the first input block of solution mappings
	protected BaseForExecOpFrawBindJoinSPARQL currentInstance = null;

	public ExecOpFrawBindJoinSPARQLwithVALUESorFILTER(final SPARQLGraphPattern query,
                                                      final SPARQLEndpoint fm,
                                                      final boolean useOuterJoinSemantics,
                                                      final boolean collectExceptions ) {
		super(query, fm, collectExceptions);
		this.useOuterJoinSemantics = useOuterJoinSemantics;
	}

	public ExecOpFrawBindJoinSPARQLwithVALUESorFILTER(final SPARQLGraphPattern query,
													  final FederationMemberAgglomeration fm,
													  final boolean useOuterJoinSemantics,
													  final boolean collectExceptions ) {
		super(query, fm, collectExceptions);
		this.useOuterJoinSemantics = useOuterJoinSemantics;
	}

	@Override
	protected void _process( final IntermediateResultBlock input,
	                         final IntermediateResultElementSink sink,
	                         final ExecutionContext execCxt ) throws ExecOpExecutionException {
		//If this is the first request
		if ( currentInstance == null ) {
			currentInstance = new ExecOpFrawBindJoinSPARQLwithVALUES(query, fm, useOuterJoinSemantics, collectExceptions);
			boolean valuesBasedRequestFailed = false;
			try {
				// Try using VALUES-based bind join
				currentInstance.process(input, sink, execCxt);
				if (!currentInstance.getExceptionsCaughtDuringExecution().isEmpty()) {
					valuesBasedRequestFailed = true;
				}	
			} catch ( final ExecOpExecutionException e ) {
				valuesBasedRequestFailed = true;
			}
			if (valuesBasedRequestFailed == true) {
				// Use FILTER-based bind join instead
				currentInstance = new ExecOpFrawBindJoinSPARQLwithFILTER(query, fm, useOuterJoinSemantics, collectExceptions);
				currentInstance.process(input, sink, execCxt);
			}
		}
		else {
			currentInstance.process(input, sink, execCxt);
		}
	}

	@Override
	protected void _concludeExecution( final IntermediateResultElementSink sink,
	                                   final ExecutionContext execCxt ) throws ExecOpExecutionException {
		if ( currentInstance != null ) {
			currentInstance.concludeExecution(sink, execCxt);
		}
	}

	@Override
	public List<Exception> getExceptionsCaughtDuringExecution() {
		if ( currentInstance != null ) {
			return currentInstance.getExceptionsCaughtDuringExecution();
		}
		else {
			return Collections.emptyList();
		}
	}

	@Override
	public void resetStats() {
		super.resetStats();

		if ( currentInstance != null ) {
			currentInstance.resetStats();
		}
	}

	@Override
	protected ExecutableOperatorStatsImpl createStats() {
		final ExecutableOperatorStatsImpl s = super.createStats();
		s.put( "currentInstance",  currentInstance.getStats() );
		return s;
	}
}
