package se.liu.ida.hefquin.engine.queryplan.executable.impl.ops;

import org.apache.jena.sparql.core.Var;
import se.liu.ida.hefquin.base.data.SolutionMapping;
import se.liu.ida.hefquin.base.query.Query;
import se.liu.ida.hefquin.engine.federation.FederationMember;
import se.liu.ida.hefquin.engine.queryplan.executable.ExecOpExecutionException;
import se.liu.ida.hefquin.engine.queryplan.executable.ExecutableOperatorStats;
import se.liu.ida.hefquin.engine.queryplan.executable.IntermediateResultElementSink;
import se.liu.ida.hefquin.engine.queryplan.executable.NullaryExecutableOp;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.FrawUtils;
import se.liu.ida.hefquin.engine.queryproc.ExecutionContext;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This abstract class's only changes in behavior compared to BaseForExecOpBindJoinWithRequestOps, is the calls
 * to merge and compatible are now made on util class FrawUtils, instead of SolutionMappingUtils.
 * This allows to handle mappings retrieved from random walks containing its retrieval probability.
 *
 * Because this behavior is contained inside inner class MyIntermediateResultElementSink, it cannot be directly overloaded
 * and as such, we need to reimplement this inner class entirely, and to keep all methods that refer to it from
 * BaseForExecOpBindJoinWithRequestOps. Not pretty.
 */
public abstract class BaseForExecOpFrawBindJoinWithRequestOps<QueryType extends Query,
                                                          MemberType extends FederationMember>
           extends BaseForExecOpBindJoinWithRequestOps<QueryType,MemberType>
{
	/**
	 * The number of solution mappings that this operator uses for each
	 * of the bind join requests. This number may be adapted at runtime.
	 */
	protected int requestBlockSize;

	/**
	 * The minimum value to which {@link #requestBlockSize} can be reduced.
	 */
	protected static final int minimumRequestBlockSize = 5;

	// statistics
	private long numberOfOutputMappingsProduced = 0L;
	protected boolean requestBlockSizeWasReduced = false;
	protected int numberOfRequestOpsUsed = 0;
	protected ExecutableOperatorStats statsOfFirstReqOp = null;
	protected ExecutableOperatorStats statsOfLastReqOp = null;

	/**
	 * @param varsInPatternForFM
	 *             may be used by sub-classes to provide the set of variables
	 *             that occur in the graph pattern that the bind join evaluates
	 *             at the federation member; sub-classes that cannot extract
	 *             this set in their constructor may pass <code>null</code> as
	 *             value for this argument; if provided, this implementation
	 *             can filter out input solution mappings that contain blank
	 *             nodes for the join variables and, thus, cannot be joined
	 *             with the solution mappings obtained via the requests
	 */
	public BaseForExecOpFrawBindJoinWithRequestOps(final QueryType query,
                                                   final MemberType fm,
                                                   final boolean useOuterJoinSemantics,
                                                   final Set<Var> varsInPatternForFM,
                                                   final boolean collectExceptions ) {
		super(query, fm, useOuterJoinSemantics, varsInPatternForFM, collectExceptions);
	}

	public BaseForExecOpFrawBindJoinWithRequestOps(final QueryType query,
                                                   final MemberType fm,
                                                   final boolean useOuterJoinSemantics,
                                                   final boolean collectExceptions ) {
		this(query, fm, useOuterJoinSemantics, null, collectExceptions);
	}

	protected void _processWithoutSplittingInputFirst( final List<SolutionMapping> joinableInputSMs,
	                                                   final IntermediateResultElementSink sink,
	                                                   final ExecutionContext execCxt ) throws ExecOpExecutionException
	{
		final NullaryExecutableOp reqOp = createExecutableReqOp(joinableInputSMs);

		if ( reqOp != null ) {
			numberOfRequestOpsUsed++;

			final MyIntermediateResultElementSink mySink;
			if ( useOuterJoinSemantics )
				mySink = new MyIntermediateResultElementSinkOuterJoin(sink, joinableInputSMs);
			else
				mySink = new MyIntermediateResultElementSink(sink, joinableInputSMs);

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

	// ------- helper classes ------

	protected class MyIntermediateResultElementSink implements IntermediateResultElementSink
	{
		protected final IntermediateResultElementSink outputSink;
		protected final Iterable<SolutionMapping> inputSolutionMappings;
		private boolean inputObtained = false;

		public MyIntermediateResultElementSink( final IntermediateResultElementSink outputSink,
		                                        final Iterable<SolutionMapping> inputSolutionMappings ) {
			this.outputSink = outputSink;
			this.inputSolutionMappings = inputSolutionMappings;
		}

		@Override
		public final void send( final SolutionMapping smFromRequest ) {
			inputObtained = true;
			_send(smFromRequest);
		}

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

		public void flush() { }

		public final boolean hasObtainedInputAlready() { return inputObtained; }

	} // end of helper class MyIntermediateResultElementSink


	protected class MyIntermediateResultElementSinkOuterJoin extends MyIntermediateResultElementSink
	{
		protected final Set<SolutionMapping> inputSolutionMappingsWithJoinPartners = new HashSet<>();

		public MyIntermediateResultElementSinkOuterJoin( final IntermediateResultElementSink outputSink,
		                                                 final Iterable<SolutionMapping> inputSolutionMappings ) {
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

		/**
		 * Sends to the output sink all input solution
		 * mappings that did not have a join partner.
		 */
		@Override
		public void flush() {
			for ( final SolutionMapping smFromInput : inputSolutionMappings ) {
				if ( ! inputSolutionMappingsWithJoinPartners.contains(smFromInput) ) {
					numberOfOutputMappingsProduced++;
					outputSink.send(smFromInput);
				}
			}
		}

	} // end of helper class MyIntermediateResultElementSinkOuterJoin

}
