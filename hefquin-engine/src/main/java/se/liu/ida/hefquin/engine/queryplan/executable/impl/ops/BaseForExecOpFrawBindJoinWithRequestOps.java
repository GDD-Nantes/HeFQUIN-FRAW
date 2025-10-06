package se.liu.ida.hefquin.engine.queryplan.executable.impl.ops;

import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import se.liu.ida.hefquin.base.data.SolutionMapping;
import se.liu.ida.hefquin.base.query.ExpectedVariables;
import se.liu.ida.hefquin.base.query.Query;
import se.liu.ida.hefquin.engine.queryplan.executable.ExecOpExecutionException;
import se.liu.ida.hefquin.engine.queryplan.executable.IntermediateResultElementSink;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.FrawUtils;
import se.liu.ida.hefquin.engine.queryplan.info.QueryPlanningInfo;
import se.liu.ida.hefquin.engine.queryproc.ExecutionContext;
import se.liu.ida.hefquin.federation.FederationMember;

import java.util.HashSet;
import java.util.Set;

/**
 * A generic implementation of (a batching version of) the bind join algorithm
 * that uses executable request operators for performing the requests to the
 * federation member.
 *
 * The implementation is generic in the sense that it works with any type of
 * request operator. Each concrete implementation that extends this base class
 * needs to implement the {@link #createExecutableReqOp(Set)} method to create
 * the request operators with the types of requests that are specific to that
 * concrete implementation.
 *
 * The algorithm collects solution mappings from the input. Once enough
 * solution mappings have arrived, the algorithm creates the corresponding
 * request (see above) and sends this request to the federation member (the
 * algorithm may even decide to split the input batch into smaller batches
 * for multiple requests; see below). The response to such a request is the
 * subset of the solutions for the query/pattern of this operator that are
 * join partners for at least one of the solutions that were used for creating
 * the request. After receiving such a response, the algorithm locally joins
 * the solutions from the response with the solutions in the batch used for
 * creating the request, and outputs the resulting joined solutions (if any).
 * Thereafter, the algorithm moves on to collect the next solution mappings
 * from the input, until it can do the next request, etc.
 *
 * This implementation is capable of separating out each input solution mapping
 * that assigns a blank node to any of the join variables. Then, such solution
 * mappings are not even considered when creating the requests because they
 * cannot have any join partners in the results obtained from the federation
 * member. Of course, in case the algorithm is used with outer-join semantics,
 * these solution mappings are still returned to the output (without joining
 * them with anything).
 *
 * A feature of this implementation is that, in case a request operator fails,
 * this implementation automatically reduces the batch size for requests and,
 * then, tries to re-process (with the reduced request batch size) the input
 * solution mappings for which the request operator failed.
 *
 * Another feature of this implementation is that it can switch into a
 * full-retrieval mode as soon as there is an input solution mapping that
 * does not have a binding for any of the join variables (which may happen
 * only in cases in which at least one of the join variables is a certain
 * variable). Such an input solution mapping is compatible with (and, thus,
 * can be joined with) every solution mapping that the federation member has
 * for the query/pattern of this bind-join operator. Therefore, when switching
 * into full-retrieval mode, this implementation performs a request to retrieve
 * the complete set of all these solution mappings and, then, uses this set to
 * find join partners for the current and the future batches of input solution
 * mappings (because, with the complete set available locally, there is no need
 * anymore to issue further bind-join requests). This capability relies on the
 * {@link #createExecutableReqOpForAll()} method that needs to be provided by
 * each concrete implementation that extends this base class.
 */
public abstract class BaseForExecOpFrawBindJoinWithRequestOps<QueryType extends Query,
		MemberType extends FederationMember> extends BaseForExecOpBindJoinWithRequestOps<QueryType, MemberType>
{

	public BaseForExecOpFrawBindJoinWithRequestOps( final QueryType query,
												final Set<Var> varsInQuery,
												final MemberType fm,
												final ExpectedVariables inputVars,
												final boolean useOuterJoinSemantics,
												final int batchSize,
												final boolean collectExceptions,
												final QueryPlanningInfo qpInfo ) {
		super(query, varsInQuery, fm, inputVars, useOuterJoinSemantics, batchSize, collectExceptions, qpInfo);
	}

	/**
	 * Makes sure that the given solution mapping will be considered for the
	 * next bind-join request, and performs that request if enough solution
	 * mappings have been accumulated.
	 *
	 * @param inputSolMap - the solution mapping to be considered; at this
	 *             point, we assume that this solution mapping covers at
	 *             least one join variable and does not assign a blank node
	 *             to any of the join variables
	 *
	 * @param inputSolMapRestricted - a version of inputSolMap that is
	 *             restricted to the join variables
	 */
	protected void _processJoinableInput( final SolutionMapping inputSolMap,
										  final Binding inputSolMapRestricted,
										  final IntermediateResultElementSink sink,
										  final ExecutionContext execCxt )
			throws ExecOpExecutionException
	{
		// Add the given solution mapping to the batch of solution mappings
		// considered by the next bind-join request.
		currentBatch.add(inputSolMap);

		// Check whether the restricted version of the given input solution
		// mapping is already covered by the set of solution mappings from
		// which the next bind-join request will be formed.
		if ( ! alreadyCovered(inputSolMapRestricted) ) {
			// If it is not covered, we need to add it to the set, but first
			// we may have to remove solution mappings from that set.
			if ( ! allJoinVarsAreCertain ) {
				// Update the set of solution mappings already collected for
				// the request by removing the solution mappings that include
				// the given restricted solution mapping.
				// This is okay because the potential join partners captured
				// by these solution mappings are also captured by the given
				// restricted solution mapping, which we will add to the set
				// in the next step. In fact, it is even necessary to do so
				// in order to avoid spurious duplicates in the join result.
				currentSolMapsForRequest.removeIf( sm -> FrawUtils.includedIn(inputSolMapRestricted, sm) );
			}

			// Now we add it to the set.
			currentSolMapsForRequest.add(inputSolMapRestricted);
		}

		performRequestAndHandleResponse(sink, execCxt);

		// After performing the request (and handling its response), we can
		// forget about the solution mappings considered for the request.
		currentSolMapsForRequest.clear();
		currentBatch.clear();
	}

	protected boolean alreadyCovered( final Binding inputSolMapRestricted ) {
		if ( currentSolMapsForRequest.contains(inputSolMapRestricted) ) {
			return true;
		}

		if ( ! allJoinVarsAreCertain ) {
			for ( final Binding sm : currentSolMapsForRequest ) {
				if ( FrawUtils.includedIn(sm, inputSolMapRestricted) ) {
					return true;
				}
			}
		}

		return false;
	}

	@Override
	protected FrawIntermediateResultElementSink createMySink() {
		if ( useOuterJoinSemantics )
			return new FrawIntermediateResultElementSinkOuterJoin(currentBatch);
		else
			return new FrawIntermediateResultElementSink(currentBatch);
	}

	// ------- helper classes ------

	protected class FrawIntermediateResultElementSink extends MyIntermediateResultElementSink
	{
		// Extra step to force Iterable<SolutionMapping> type on the inputSolutionMappings argument, instead of using
		// the attribute from parent class

		public FrawIntermediateResultElementSink( final Iterable<SolutionMapping> inputSolutionMappings ) {
            super(inputSolutionMappings);
		}

		@Override
		protected void _send( final SolutionMapping smFromRequest ) {
			for ( final SolutionMapping smFromInput : inputSolutionMappings ) {
				if ( FrawUtils.compatible(smFromInput, smFromRequest) ) {
					solMapsForOutput.add( FrawUtils.merge(smFromInput,smFromRequest, useOuterJoinSemantics) );
				}
			}
		}

	} // end of helper class MyIntermediateResultElementSink


	protected class FrawIntermediateResultElementSinkOuterJoin extends FrawIntermediateResultElementSink
	{
		protected final Set<SolutionMapping> inputSolutionMappingsWithJoinPartners = new HashSet<>();

		public FrawIntermediateResultElementSinkOuterJoin( final Iterable<SolutionMapping> inputSolutionMappings ) {
			super(inputSolutionMappings);
		}

		@Override
		public void _send( final SolutionMapping smFromRequest ) {
			for ( final SolutionMapping smFromInput : inputSolutionMappings ) {
				if ( FrawUtils.compatible(smFromInput, smFromRequest) ) {
					solMapsForOutput.add( FrawUtils.merge(smFromInput,smFromRequest,useOuterJoinSemantics) );
					inputSolutionMappingsWithJoinPartners.add(smFromInput);
				}
			}
		}

		/**
		 * Sends to the output sink all input solution
		 * mappings that did not have a join partner.
		 */
		@Override
		public void flush() {
			for ( final SolutionMapping smFromInput : inputSolutionMappings ) {
				if ( ! inputSolutionMappingsWithJoinPartners.contains(smFromInput) ) {
					solMapsForOutput.add(smFromInput);
				}
			}
		}

	} // end of helper class MyIntermediateResultElementSinkOuterJoin
}
