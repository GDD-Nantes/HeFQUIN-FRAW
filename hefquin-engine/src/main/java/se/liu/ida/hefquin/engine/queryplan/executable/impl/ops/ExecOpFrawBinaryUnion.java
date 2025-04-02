package se.liu.ida.hefquin.engine.queryplan.executable.impl.ops;

import org.apache.jena.sparql.engine.binding.Binding;
import se.liu.ida.hefquin.base.data.SolutionMapping;
import se.liu.ida.hefquin.base.data.impl.SolutionMappingImpl;
import se.liu.ida.hefquin.engine.queryplan.executable.IntermediateResultBlock;
import se.liu.ida.hefquin.engine.queryplan.executable.IntermediateResultElementSink;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.CollectingIntermediateResultElementSink;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.FrawUtils;
import se.liu.ida.hefquin.engine.queryproc.ExecutionContext;

public class ExecOpFrawBinaryUnion extends ExecOpBinaryUnion {

    public ExecOpFrawBinaryUnion(int numberOfChildren, boolean collectExceptions) {
        super(collectExceptions);
    }


    @Override
    protected void _processBlockFromChild1( final IntermediateResultBlock input,
                                            final IntermediateResultElementSink sink,
                                            final ExecutionContext execCxt ) {
        CollectingIntermediateResultElementSink tempSink = new CollectingIntermediateResultElementSink();

        super._processBlockFromChild1(input, tempSink, execCxt);

        tempSink.getCollectedSolutionMappings().forEach(
                solutionMapping -> {
                    Binding updatedBinding = FrawUtils.updateProbaUnion(solutionMapping, 2, 0);
                    SolutionMapping updatedSolutionMapping = new SolutionMappingImpl(updatedBinding);
                    sink.send(updatedSolutionMapping);
                }
        );
    }

    @Override
    protected void _processBlockFromChild2( final IntermediateResultBlock input,
                                            final IntermediateResultElementSink sink,
                                            final ExecutionContext execCxt ) {
        CollectingIntermediateResultElementSink tempSink = new CollectingIntermediateResultElementSink();

        super._processBlockFromChild1(input, tempSink, execCxt);

        tempSink.getCollectedSolutionMappings().forEach(
                solutionMapping -> {
                    Binding updatedBinding = FrawUtils.updateProbaUnion(solutionMapping, 2, 1);
                    SolutionMapping updatedSolutionMapping = new SolutionMappingImpl(updatedBinding);
                    sink.send(updatedSolutionMapping);
                }
        );
    }


//    @Override
//    protected void _processBlockFromXthChild( final int x,
//                                              final IntermediateResultBlock input,
//                                              final IntermediateResultElementSink sink,
//                                              final ExecutionContext execCxt) {
//
//        CollectingIntermediateResultElementSink tempSink = new CollectingIntermediateResultElementSink();
//
//        super._processBlockFromXthChild(x, input, tempSink, execCxt);
//
//        tempSink.getCollectedSolutionMappings().forEach(
//                solutionMapping -> {
//                    Binding updatedBinding = FrawUtils.updateProbaUnion(solutionMapping, numberOfChildren, x);
//                    SolutionMapping updatedSolutionMapping = new SolutionMappingImpl(updatedBinding);
//                    sink.send(updatedSolutionMapping);
//                }
//        );
//    }
//
//    @Override
//    protected void _wrapUpForXthChild(int x,
//                                      IntermediateResultElementSink sink,
//                                      ExecutionContext execCxt) {
//
//    }
}