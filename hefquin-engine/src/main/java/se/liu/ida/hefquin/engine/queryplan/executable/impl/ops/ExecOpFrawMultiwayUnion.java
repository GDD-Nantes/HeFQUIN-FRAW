package se.liu.ida.hefquin.engine.queryplan.executable.impl.ops;

import org.apache.jena.sparql.engine.binding.Binding;
import se.liu.ida.hefquin.base.data.SolutionMapping;
import se.liu.ida.hefquin.base.data.impl.SolutionMappingImpl;
import se.liu.ida.hefquin.engine.queryplan.executable.IntermediateResultElementSink;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.CollectingIntermediateResultElementSink;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.FrawUtils;
import se.liu.ida.hefquin.engine.queryplan.info.QueryPlanningInfo;
import se.liu.ida.hefquin.engine.queryproc.ExecutionContext;

import java.util.List;

public class ExecOpFrawMultiwayUnion extends ExecOpMultiwayUnion{

    public ExecOpFrawMultiwayUnion(int numberOfChildren, boolean collectExceptions, QueryPlanningInfo qpInfo) {
        super(numberOfChildren, collectExceptions, qpInfo);
    }

    @Override
    protected void _processInputFromXthChild( final int x,
                                              final SolutionMapping inputSolMap,
                                              final IntermediateResultElementSink sink,
                                              final ExecutionContext execCxt) {
        CollectingIntermediateResultElementSink tempSink = new CollectingIntermediateResultElementSink();

        super._processInputFromXthChild(x, inputSolMap, tempSink, execCxt);

        tempSink.getCollectedSolutionMappings().forEach(
                solutionMapping -> {
                    Binding updatedBinding = FrawUtils.updateProbaUnion(solutionMapping, numberOfChildren, x);
                    SolutionMapping updatedSolutionMapping = new SolutionMappingImpl(updatedBinding);
                    sink.send(updatedSolutionMapping);
                }
        );
    }

    @Override
    protected void _processInputFromXthChild( final int x,
                                              final List<SolutionMapping> inputSolMaps,
                                              final IntermediateResultElementSink sink,
                                              final ExecutionContext execCxt) {
        CollectingIntermediateResultElementSink tempSink = new CollectingIntermediateResultElementSink();

        super._processInputFromXthChild(x, inputSolMaps, tempSink, execCxt);

        tempSink.getCollectedSolutionMappings().forEach(
                solutionMapping -> {
                    Binding updatedBinding = FrawUtils.updateProbaUnion(solutionMapping, numberOfChildren, x);
                    SolutionMapping updatedSolutionMapping = new SolutionMappingImpl(updatedBinding);
                    sink.send(updatedSolutionMapping);
                }
        );
    }
}