package se.liu.ida.hefquin.jenaintegration.sparql.engine.main;

import org.apache.jena.query.QueryExecException;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.algebra.walker.WalkerVisitorSkipService;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIter;
import org.apache.jena.sparql.engine.iterator.QueryIterNullIterator;
import org.apache.jena.sparql.engine.iterator.QueryIterRepeatApply;
import org.apache.jena.sparql.engine.main.OpExecutor;
import se.liu.ida.hefquin.base.data.SolutionMapping;
import se.liu.ida.hefquin.base.data.impl.SolutionMappingImpl;
import se.liu.ida.hefquin.base.query.SPARQLGraphPattern;
import se.liu.ida.hefquin.base.query.VariableByBlankNodeSubstitutionException;
import se.liu.ida.hefquin.base.query.impl.GenericSPARQLGraphPatternImpl2;
import se.liu.ida.hefquin.base.query.utils.QueryPatternUtils;
import se.liu.ida.hefquin.engine.QueryIterGroupFraw;
import se.liu.ida.hefquin.engine.QueryProcessingStatsAndExceptions;
import se.liu.ida.hefquin.engine.queryproc.QueryProcException;
import se.liu.ida.hefquin.engine.queryproc.SamplingQueryProcessor;
import se.liu.ida.hefquin.engine.queryproc.impl.MaterializingQueryResultSinkWithInputBindingImpl;
import se.liu.ida.hefquin.jenaintegration.sparql.FrawConstants;
import se.liu.ida.hefquin.jenaintegration.sparql.HeFQUINConstants;

import java.util.Iterator;
import java.util.Objects;

public class OpExecutorFraw extends OpExecutor
{
	protected final SamplingQueryProcessor qProc;
	protected boolean nextQueryProcSingleWalk = false;
	protected final int budget;
	protected final int subBudget;

	public OpExecutorFraw(final SamplingQueryProcessor qProc, final ExecutionContext execCxt, final int budget, final int subBudget ) {
		super(execCxt);

		assert qProc != null;
		this.qProc = qProc;

		assert budget >= 0;
		assert subBudget >= 0;
		this.budget = budget;
		this.subBudget = subBudget;
	}

	@Override
	protected QueryIterator exec(Op op, QueryIterator input) {
		return super.exec(op, input);
	}

	@Override
	protected QueryIterator execute( final OpBGP opBGP, final QueryIterator input ) {
		if ( isSupportedOp(opBGP) ) {
			return executeSupportedOp( opBGP, input );
		}
		else {
			return super.execute(opBGP, input);
		}
	}

	@Override
	protected QueryIterator execute( final OpSequence opSequence, final QueryIterator input ) {
		if ( isSupportedOp(opSequence) ) {
			return executeSupportedOp( opSequence, input );
		}
		else {
			return super.execute(opSequence, input);
		}
	}

	@Override
	protected QueryIterator execute(final OpJoin opJoin, final QueryIterator input ) {
		if ( isSupportedOp(opJoin) ) {
			return executeSupportedOp( opJoin, input );
		}
		else {
			// Forcing use of bound join; it's random walk we're talking about after all
			QueryIterator left = exec(opJoin.getLeft(), input);
			this.nextQueryProcSingleWalk = true;
			return super.executeOp(opJoin.getRight(), left);
		}
	}

	@Override
	protected QueryIterator execute( final OpLeftJoin opLeftJoin, final QueryIterator input ) {
		if ( isSupportedOp(opLeftJoin) ) {
			return executeSupportedOp( opLeftJoin, input );
		}
		else {
			return super.execute(opLeftJoin, input);
		}
	}

	@Override
	protected QueryIterator execute( final OpUnion opUnion, final QueryIterator input ) {
		if ( isSupportedOp(opUnion) ) {
			return executeSupportedOp( opUnion, input );
		}
		else {
			return super.execute(opUnion, input);
		}
	}

	@Override
	protected QueryIterator execute( final OpConditional opConditional, final QueryIterator input ) {
		if ( isSupportedOp(opConditional) ) {
			return executeSupportedOp( opConditional, input );
		}
		else {
			return super.execute(opConditional, input);
		}
	}

	@Override
	protected QueryIterator execute( final OpExtend opExtend, final QueryIterator input ) {
		if ( isSupportedOp(opExtend) ) {
			return executeSupportedOp( opExtend, input );
		}
		else {
			return super.execute(opExtend, input);
		}
	}

	@Override
	protected QueryIterator execute( final OpFilter opFilter, final QueryIterator input ) {
		if ( isSupportedOp(opFilter) ) {
			return executeSupportedOp( opFilter, input );
		}
		else {
			return super.execute(opFilter, input);
		}
	}

	@Override
	protected QueryIterator execute( final OpService opService, final QueryIterator input ) {
		if ( isSupportedOp(opService) ) {
			return executeSupportedOp( opService, input );
		}
		else {
			throw new UnsupportedOperationException();
		}
	}

	@Override
	protected QueryIterator execute(OpGroup opGroup, QueryIterator input) {

		if ( isSupportedOp(opGroup) ) {
			return executeSupportedOp( opGroup, input );
		}
		else {
			QueryIterator qIter = exec(opGroup.getSubOp(), input);
			return new QueryIterGroupFraw(qIter, opGroup.getGroupVars(), opGroup.getAggregators(), execCxt);
		}
	}


	protected boolean isSupportedOp( final Op op ) {
		final UnsupportedOpFinder f = new UnsupportedOpFinder();
		new WalkerVisitorSkipService(f, null, null, null).walk(op);
		final boolean unsupportedOpFound = f.unsupportedOpFound();
		return ! unsupportedOpFound;
	}

	protected QueryIterator executeSupportedOp( final Op op, final QueryIterator input ) {
		Integer queryBudget;
		try {
			Integer customQueryBudget = execCxt.getContext().get(FrawConstants.BUDGET);
			queryBudget = customQueryBudget == null ? budget : Math.min(customQueryBudget, budget);
		} catch ( Exception e ) {
			queryBudget = budget;
		}

		Integer querySubBudget;
		try {
			Integer customQuerySubBudget = execCxt.getContext().get(FrawConstants.SUB_BUDGET);
			querySubBudget = customQuerySubBudget == null ? subBudget : Math.min(customQuerySubBudget, subBudget);
		} catch ( Exception e ) {
			querySubBudget = subBudget;
		}

		queryBudget = Math.max(queryBudget, 1);
		querySubBudget = Math.max(querySubBudget, 1);

		return new MainQueryIterator( op, input, nextQueryProcSingleWalk ? querySubBudget : queryBudget );
	}


	protected class MainQueryIterator extends QueryIterRepeatApply
	{
		protected final Op op;
		protected final int numberOfWalks;

		public MainQueryIterator( final Op op, final QueryIterator input ) {
			super(input, execCxt);

			throw new QueryExecException("Can't instantiate MainQueryIterator without a budget");
		}

		public MainQueryIterator( final Op op, final QueryIterator input, final int numberOfWalks ) {
			super(input, execCxt);

			assert op != null;
			this.op = op;

			this.numberOfWalks = numberOfWalks;
		}

		@Override
		protected QueryIterator nextStage( final Binding binding ) {
			final Op opForStage;

			if ( binding.isEmpty() ) {
				opForStage = op;
			}
			else {
				SPARQLGraphPattern unboundSgp = new GenericSPARQLGraphPatternImpl2(op);
				SolutionMapping sm = new SolutionMappingImpl(binding);
				SPARQLGraphPattern boundSGP;

				try {
					boundSGP = QueryPatternUtils.applySolMapToGraphPattern(sm, unboundSgp);
				} catch (VariableByBlankNodeSubstitutionException e) {
					throw new RuntimeException(e);
				}

				opForStage = QueryPatternUtils.convertToJenaOp(boundSGP);
			}

			final MaterializingQueryResultSinkWithInputBindingImpl sink = new MaterializingQueryResultSinkWithInputBindingImpl(binding);
			final QueryProcessingStatsAndExceptions queryProcessingStatsAndExceptions;

			try {
				queryProcessingStatsAndExceptions = qProc.processQuery( new GenericSPARQLGraphPatternImpl2(opForStage), sink, numberOfWalks );
				if(Objects.isNull(queryProcessingStatsAndExceptions)) return new QueryIterNullIterator(execCxt);
			}
			catch ( final QueryProcException ex ) {
				throw new QueryExecException("Processing the query operator using HeFQUIN failed.", ex);
			}

			execCxt.getContext().set( HeFQUINConstants.sysQProcStatsAndExceptions,
					queryProcessingStatsAndExceptions );

			return new WrappingQueryIterator( sink.getSolMapsIter() );
		}
	}


	protected class WrappingQueryIterator extends QueryIter
	{
		protected final Iterator<SolutionMapping> it;

		public WrappingQueryIterator( final Iterator<SolutionMapping> it ) {
			super(execCxt);
			this.it = it;
		}

		@Override
		protected boolean hasNextBinding() { return it.hasNext(); }

		@Override
		protected Binding moveToNextBinding() { return it.next().asJenaBinding(); }

		@Override
		protected void closeIterator() {} // nothing to do here

		@Override
		protected void requestCancel() {} // nothing to do here
	}

	protected static class UnsupportedOpFinder extends OpVisitorBase
	{
		protected boolean unsupportedOpFound = false;

		public boolean unsupportedOpFound() { return unsupportedOpFound; }

		@Override public void visit(OpBGP opBGP)                  {}

		@Override public void visit(OpQuadPattern quadPattern)    { unsupportedOpFound = true; }

	    @Override public void visit(OpQuadBlock quadBlock)        { unsupportedOpFound = true; }

	    @Override public void visit(OpTriple opTriple)            { unsupportedOpFound = true; }

	    @Override public void visit(OpQuad opQuad)                { unsupportedOpFound = true; }

	    @Override public void visit(OpPath opPath)                { unsupportedOpFound = true; }

	    @Override public void visit(OpProcedure opProc)           { unsupportedOpFound = true; }

	    @Override public void visit(OpPropFunc opPropFunc)        { unsupportedOpFound = true; }

	    @Override public void visit(OpJoin opJoin)                {} // supported

	    @Override public void visit(OpSequence opSequence)        {} // supported

	    @Override public void visit(OpDisjunction opDisjunction)  { unsupportedOpFound = true; }

		@Override public void visit(OpLeftJoin opLeftJoin)        {} // supported

	    @Override public void visit(OpConditional opCond)         {} // supported

	    @Override public void visit(OpMinus opMinus)              { unsupportedOpFound = true; }

	    @Override public void visit(OpDiff opDiff)                { unsupportedOpFound = true; }

	    @Override public void visit(OpUnion opUnion)              {} // supported

	    @Override public void visit(OpFilter opFilter)            {} // supported

	    @Override public void visit(OpGraph opGraph)              { unsupportedOpFound = true; }

	    @Override public void visit(OpService opService)          {} // supported

	    @Override public void visit(OpDatasetNames dsNames)       { unsupportedOpFound = true; }

	    @Override public void visit(OpTable opTable)              { unsupportedOpFound = true; }

	    @Override public void visit(OpExt opExt)                  { unsupportedOpFound = true; }

	    @Override public void visit(OpNull opNull)                { unsupportedOpFound = true; }

	    @Override public void visit(OpLabel opLabel)              { unsupportedOpFound = true; }

	    @Override public void visit(OpAssign opAssign)            { unsupportedOpFound = true; }

	    @Override public void visit(OpExtend opExtend)            {} // supported

	    //@Override public void visit(OpFind opFind)                { unsupportedOpFound = true; }

	    @Override public void visit(OpList opList)                { unsupportedOpFound = true; }

	    @Override public void visit(OpOrder opOrder)              { unsupportedOpFound = true; }

	    @Override public void visit(OpProject opProject)          { unsupportedOpFound = true; }

	    @Override public void visit(OpDistinct opDistinct)        { unsupportedOpFound = true; }

	    @Override public void visit(OpReduced opReduced)          { unsupportedOpFound = true; }

	    @Override public void visit(OpSlice opSlice)              { unsupportedOpFound = true; }

	    @Override public void visit(OpGroup opGroup)              { unsupportedOpFound = true; }

	    @Override public void visit(OpTopN opTop)                 { unsupportedOpFound = true; }
	}

}
