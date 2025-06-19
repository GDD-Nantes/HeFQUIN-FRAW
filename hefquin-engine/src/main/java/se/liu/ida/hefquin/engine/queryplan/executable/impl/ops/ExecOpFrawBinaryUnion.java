package se.liu.ida.hefquin.engine.queryplan.executable.impl.ops;

import org.apache.jena.sparql.engine.binding.Binding;
import se.liu.ida.hefquin.base.data.SolutionMapping;
import se.liu.ida.hefquin.base.data.impl.SolutionMappingImpl;
import se.liu.ida.hefquin.engine.queryplan.executable.IntermediateResultElementSink;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.CollectingIntermediateResultElementSink;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.FrawUtils;
import se.liu.ida.hefquin.engine.queryproc.ExecutionContext;

import java.util.List;

public class ExecOpFrawBinaryUnion extends ExecOpBinaryUnion {

    public ExecOpFrawBinaryUnion(boolean collectExceptions) {
        super(collectExceptions);
    }


    @Override
    protected void _processInputFromChild1( final SolutionMapping inputSolMap,
                                            final IntermediateResultElementSink sink,
                                            final ExecutionContext execCxt ) {
        CollectingIntermediateResultElementSink tempSink = new CollectingIntermediateResultElementSink();

        super._processInputFromChild1(inputSolMap, tempSink, execCxt);

        tempSink.getCollectedSolutionMappings().forEach(
                solutionMapping -> {
                    Binding updatedBinding = FrawUtils.updateProbaUnion(solutionMapping, 2, 0);
                    SolutionMapping updatedSolutionMapping = new SolutionMappingImpl(updatedBinding);
                    sink.send(updatedSolutionMapping);
                }
        );
    }

    @Override
    protected void _processInputFromChild1( final List<SolutionMapping> inputSolMaps,
                                            final IntermediateResultElementSink sink,
                                            final ExecutionContext execCxt ) {
        CollectingIntermediateResultElementSink tempSink = new CollectingIntermediateResultElementSink();

        super._processInputFromChild1(inputSolMaps, tempSink, execCxt);

        tempSink.getCollectedSolutionMappings().forEach(
                solutionMapping -> {
                    Binding updatedBinding = FrawUtils.updateProbaUnion(solutionMapping, 2, 1);
                    SolutionMapping updatedSolutionMapping = new SolutionMappingImpl(updatedBinding);
                    sink.send(updatedSolutionMapping);
                }
        );
    }

    @Override
    protected void _processInputFromChild2( final SolutionMapping inputSolMap,
                                            final IntermediateResultElementSink sink,
                                            final ExecutionContext execCxt ) {
        CollectingIntermediateResultElementSink tempSink = new CollectingIntermediateResultElementSink();

        super._processInputFromChild2(inputSolMap, tempSink, execCxt);

        tempSink.getCollectedSolutionMappings().forEach(
                solutionMapping -> {
                    Binding updatedBinding = FrawUtils.updateProbaUnion(solutionMapping, 2, 0);
                    SolutionMapping updatedSolutionMapping = new SolutionMappingImpl(updatedBinding);
                    sink.send(updatedSolutionMapping);
                }
        );
    }

    @Override
    protected void _processInputFromChild2( final List<SolutionMapping> inputSolMaps,
                                            final IntermediateResultElementSink sink,
                                            final ExecutionContext execCxt ) {
        CollectingIntermediateResultElementSink tempSink = new CollectingIntermediateResultElementSink();

        super._processInputFromChild2(inputSolMaps, tempSink, execCxt);

        tempSink.getCollectedSolutionMappings().forEach(
                solutionMapping -> {
                    Binding updatedBinding = FrawUtils.updateProbaUnion(solutionMapping, 2, 1);
                    SolutionMapping updatedSolutionMapping = new SolutionMappingImpl(updatedBinding);
                    sink.send(updatedSolutionMapping);
                }
        );
    }
}