package se.liu.ida.hefquin.jenaintegration.sparql.engine.main;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.query.QueryExecException;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.algebra.walker.WalkerVisitorSkipService;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingLib;
import org.apache.jena.sparql.engine.iterator.*;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.serializer.SerializationContext;
import se.liu.ida.hefquin.base.data.SolutionMapping;
import se.liu.ida.hefquin.base.data.impl.SolutionMappingImpl;
import se.liu.ida.hefquin.base.data.utils.SolutionMappingUtils;
import se.liu.ida.hefquin.base.query.SPARQLGraphPattern;
import se.liu.ida.hefquin.base.query.impl.GenericSPARQLGraphPatternImpl2;
import se.liu.ida.hefquin.base.query.impl.QueryPatternUtils;
import se.liu.ida.hefquin.base.utils.Pair;
import se.liu.ida.hefquin.engine.queryproc.QueryProcException;
import se.liu.ida.hefquin.engine.queryproc.QueryProcStats;
import se.liu.ida.hefquin.engine.queryproc.QueryProcessor;
import se.liu.ida.hefquin.engine.queryproc.impl.MaterializingQueryResultSinkImpl;
import se.liu.ida.hefquin.engine.queryproc.impl.QueryProcessorImpl;
import se.liu.ida.hefquin.engine.queryproc.impl.compiler.PushBasedSamplingQueryPlanCompilerImpl;
import se.liu.ida.hefquin.jenaintegration.sparql.HeFQUINConstants;

import java.util.Iterator;
import java.util.List;

public class OpExecutorFraw extends OpExecutor {
    protected final QueryProcessor qProc;
    private static int keyExploration = 50;
    private static int keyExploitation = 50;
    private QueryProcessor keyExplorationQueryProcessor;
    private QueryProcessor keyExploitationQueryProcessor;

    public OpExecutorFraw(final QueryProcessor qProc, final ExecutionContext execCxt ) {
        super(execCxt);

        assert qProc != null;
        this.qProc = qProc;

        keyExplorationQueryProcessor = new QueryProcessorImpl(
                qProc.getPlanner(),
                new PushBasedSamplingQueryPlanCompilerImpl(
                        ((PushBasedSamplingQueryPlanCompilerImpl) qProc.getPlanCompiler()).getQueryProcContext(),
                        keyExploration
                ),
                qProc.getExecutionEngine(),
                ((PushBasedSamplingQueryPlanCompilerImpl) qProc.getPlanCompiler()).getQueryProcContext()
        );

        keyExploitationQueryProcessor = new QueryProcessorImpl(
                qProc.getPlanner(),
                new PushBasedSamplingQueryPlanCompilerImpl(
                        ((PushBasedSamplingQueryPlanCompilerImpl) qProc.getPlanCompiler()).getQueryProcContext(),
                        keyExploitation
                ),
                qProc.getExecutionEngine(),
                ((PushBasedSamplingQueryPlanCompilerImpl) qProc.getPlanCompiler()).getQueryProcContext()
        );
    }

    @Override
    protected QueryIterator exec(Op op, QueryIterator input) {
        return super.exec(op, input);
    }

    @Override
    protected QueryIterator execute(final OpBGP opBGP, final QueryIterator input ) {
        if ( isSupportedOp(opBGP) ) {
            return executeSupportedOp( opBGP, input );
        }
        else {
            return super.execute(opBGP, input);
        }
    }

    @Override
    protected QueryIterator execute(final OpSequence opSequence, final QueryIterator input ) {
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

            // For each row, substitute and execute.
            QueryIterator qIter = new FrawQueryIterLateral(left, opJoin.getRight(), execCxt);
            return qIter;
        }
    }

    @Override
    protected QueryIterator execute(final OpLeftJoin opLeftJoin, final QueryIterator input ) {
        if ( isSupportedOp(opLeftJoin) ) {
            return executeSupportedOp( opLeftJoin, input );
        }
        else {
            return super.execute(opLeftJoin, input);
        }
    }

    @Override
    protected QueryIterator execute(final OpUnion opUnion, final QueryIterator input ) {
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
            // Key exploration
            // If the input already provides a full key binding, we don't need to explore
            // What if input iterates over multiple input bindings?
//            boolean shouldExplore = false;
//            if(input.hasNext()){
//                Binding inputBinding  = input.next();
//                for(Var var : opGroup.getGroupVars().getVars()){
//                    if(!inputBinding.contains(var)){
//                        shouldExplore = true;
//                    }
//                }
//            }
//            QueryIterator inputKeyExplorationBindings = shouldExplore ? this.exec(opGroup.getSubOp(), input) : null;


            // Per-key exploitation
            QueryIterator bindingsToGroup = executeSupportedOp( opGroup, input );

            // Grouping and accumulation
            QueryIterator qIter = new QueryIterGroup(bindingsToGroup, opGroup.getGroupVars(), opGroup.getAggregators(), execCxt);
            return qIter;

        }
    }


    protected boolean isSupportedOp( final Op op ) {
        final UnsupportedOpFinder f = new UnsupportedOpFinder();
        new WalkerVisitorSkipService(f, null, null, null).walk(op);
        final boolean unsupportedOpFound = f.unsupportedOpFound();
        return ! unsupportedOpFound;
    }

    protected QueryIterator executeSupportedOp( final Op op, final QueryIterator input ) {
        return new MainQueryIterator( op, input );
    }

    protected QueryIterator executeSupportedOp( final OpGroup opGroup, final QueryIterator input ) {
        QueryIterator inputKeyExplorationBindings = new MainQueryIterator( opGroup, input );

        return new FrawGroupIterator( opGroup.getSubOp(), inputKeyExplorationBindings, opGroup.getGroupVars().getVars() );
    }

    protected class MainQueryIterator extends QueryIterRepeatApply
    {
        protected final Op op;

        public MainQueryIterator( final Op op, final QueryIterator input ) {
            super(input, execCxt);

            assert op != null;
            this.op = op;
        }

        protected QueryIterator nextStageWithCustomQueryProcessor( final Binding binding, final QueryProcessor queryProcessor) {
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
                } catch (QueryPatternUtils.VariableByBlankNodeSubstitutionException e) {
                    throw new RuntimeException(e);
                }

                opForStage = QueryPatternUtils.convertToJenaOp(boundSGP);
            }

            final MaterializingQueryResultSinkImpl sink = new MaterializingQueryResultSinkImpl();
            final Pair<QueryProcStats, List<Exception>> statsAndExceptions;

            try {
                statsAndExceptions = queryProcessor.processQuery( new GenericSPARQLGraphPatternImpl2(opForStage), sink );
            }
            catch ( final QueryProcException ex ) {
                throw new QueryExecException("Processing the query operator using HeFQUIN failed.", ex);
            }

            execCxt.getContext().set( HeFQUINConstants.sysQueryProcStats,      statsAndExceptions.object1 );
            execCxt.getContext().set( HeFQUINConstants.sysQueryProcExceptions, statsAndExceptions.object2 );

            return new WrappingQueryIterator( sink.getSolMapsIter() );
        }

        @Override
        protected QueryIterator nextStage( final Binding binding ) {
            return nextStageWithCustomQueryProcessor(binding, qProc);
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


    protected class FrawGroupIterator extends MainQueryIterator{

        private final List<Var> groupVars;

        public FrawGroupIterator(Op op, QueryIterator input, List<Var> groupVars) {
            super(op, input);
            this.groupVars = groupVars;
        }

        @Override
        protected QueryIterator nextStage( final Binding binding ) {
            Binding restricted = SolutionMappingUtils.restrict(binding, groupVars);

            // We want to use the binding retrieve during key exploration. However, its associated probability will be global, not related to the group
            // We can handle this but it requires a much heavier machinery than just one probability per mapping
            // TODO : exploit the initial binding retrieved during key exploration
//            QueryIterator inputBindingIterator = new WrappingQueryIterator(Iter.of(new SolutionMappingImpl(binding)));
            QueryIterator keyExploitationBindingsIterator = super.nextStageWithCustomQueryProcessor(restricted, keyExploitationQueryProcessor);

//            QueryIterator resultsForKeyAndInitialExploreBinding = new QueryIteratorChainer(List.of(inputBindingIterator, keyExploitationBindingsIterator));

//            return resultsForKeyAndInitialExploreBinding;
            return new FrawWrappingGroupIterator(keyExploitationBindingsIterator, restricted);
        }

        protected class FrawWrappingGroupIterator extends QueryIteratorBase{
            QueryIterator wrapped;
            Binding key;

            public FrawWrappingGroupIterator(QueryIterator wrapped, Binding key) {
                this.wrapped = wrapped;
                this.key = key;
            }

            @Override
            public Binding moveToNextBinding() {
                return BindingLib.merge(wrapped.nextBinding(), key);
            }

            @Override
            public boolean hasNextBinding() {
                return wrapped.hasNext();
            }

            // Nothing-to-dos
            @Override public void output(IndentedWriter out, SerializationContext sCxt) {}
            @Override protected void closeIterator() {}
            @Override protected void requestCancel() {}
        }
    }

    protected class FrawQueryIterLateral extends QueryIterLateral {

        public FrawQueryIterLateral(QueryIterator input, Op lateralOp, ExecutionContext execCxt) {
            super(input, lateralOp, execCxt);
        }

        @Override
        protected QueryIterator nextStage(Binding binding) {
            return new FrawWrappingQueryIterLiteral(super.nextStage(binding), binding);
        }

        protected class FrawWrappingQueryIterLiteral extends QueryIteratorBase{
            // Looks a suspicious lot like FrawWrappingGroupIterator

            Binding binding;
            QueryIterator wrapped;

            public FrawWrappingQueryIterLiteral(QueryIterator queryIterator, Binding binding) {
                this.binding = binding;
                this.wrapped = queryIterator;
            }

            @Override
            protected boolean hasNextBinding() {
                return wrapped.hasNext();
            }

            @Override
            protected Binding moveToNextBinding() {
                return BindingLib.merge(wrapped.nextBinding(), binding);
            }

            // Nothing-to-dos
            @Override public void output(IndentedWriter out, SerializationContext sCxt) {}
            @Override protected void closeIterator() {}
            @Override protected void requestCancel() {}
        }
    }

    // Might come in handy when taking care of T0D0 line 326
//    protected class QueryIteratorChainer implements QueryIterator{
//
//        Queue<QueryIterator> iterators;
//
//        public QueryIteratorChainer(List<QueryIterator> iterators) {
//            this.iterators = new LinkedList<>(iterators);
//        }
//
//        @Override
//        public Binding nextBinding() {
//            return this.next();
//        }
//
//        @Override
//        public void cancel() {
//
//        }
//
//        @Override
//        public boolean hasNext() {
//            while(true){
//                try {
//                    QueryIterator iterator = iterators.element();
//                    if (iterator.hasNext()) return true;
//                    iterators.remove();
//                } catch (Exception e){
//                    return false;
//                }
//            }
//        }
//
//        @Override
//        public Binding next() {
//            while (true){
//                QueryIterator nextIterator = iterators.element();
//                try{
//                    return nextIterator.next();
//                } catch (Exception e){
//                    iterators.remove();
//                }
//            }
//        }
//
//        @Override
//        public void output(IndentedWriter out) {
//
//        }
//
//        @Override
//        public void close() {
//
//        }
//
//        @Override
//        public void output(IndentedWriter out, SerializationContext sCxt) {
//
//        }
//
//        @Override
//        public String toString(PrefixMapping pmap) {
//            return "Custom chainer iterator";
//        }
//    }

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

//        @Override public void visit(OpGroup opGroup)              {} // supported
        @Override public void visit(OpGroup opGroup)              { unsupportedOpFound = true; }

        @Override public void visit(OpTopN opTop)                 { unsupportedOpFound = true; }
    }

}
