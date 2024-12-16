package se.liu.ida.hefquin.engine.queryplan.executable.impl.pushbased;

import se.liu.ida.hefquin.engine.queryplan.executable.ExecOpExecutionException;
import se.liu.ida.hefquin.engine.queryplan.executable.ExecutableOperator;
import se.liu.ida.hefquin.engine.queryplan.executable.IntermediateResultElementSink;
import se.liu.ida.hefquin.engine.queryplan.executable.NullaryExecutableOp;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ExecPlanTaskInputException;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ExecPlanTaskInterruptionException;
import se.liu.ida.hefquin.engine.queryproc.ExecutionContext;

import java.util.Arrays;

public class PushBasedExecPlanSamplingTaskForNullaryOperator extends PushBasedExecPlanSamplingTaskBase{

    protected final NullaryExecutableOp op;

    public PushBasedExecPlanSamplingTaskForNullaryOperator( final NullaryExecutableOp op,
                                                    final ExecutionContext execCxt,
                                                    final int minimumBlockSize ) {
        super(execCxt, minimumBlockSize);

        assert op != null;
        this.op = op;
    }

    @Override
    protected ExecutableOperator getExecOp() {
        return op;
    }

    @Override
    protected void produceOutput( final IntermediateResultElementSink sink )
            throws ExecOpExecutionException, ExecPlanTaskInputException, ExecPlanTaskInterruptionException {
        boolean failed = false;

        try {
            op.execute(sink, execCxt);
        }
        catch ( final ExecOpExecutionException e ) {
            setCauseOfFailure(e);
            failed = true;
        }

        wrapUpBatch(failed, false);

    protected void produceOutputOneMapping( final IntermediateResultElementSink sink ) throws ExecOpExecutionException {
        Random rand = new Random();

        CollectingIntermediateResultElementSink tempSink = new CollectingIntermediateResultElementSink();
        op.execute(tempSink, execCxt);
        List<SolutionMapping> tempResults = (ArrayList) tempSink.getCollectedSolutionMappings();

        // in the implementation, getCollectedSolutionMappings always return an ArrayList, so this cast shouldn't be an
        // issue. Still, it would be better to have a dedicated sink that returns specifically a List
        // TODO :
        sink.send( tempResults.get( rand.nextInt( tempResults.size() ) ) );
    }

    @Override
    protected void propagateNextBatch() {
        synchronized (availableResultBlocks){
            this.setStatus(Status.READY_NEXT_BATCH);
        }
    }

    @Override
    protected boolean isPreviousBatchDone() {
        synchronized (availableResultBlocks){
            return getStatus() == Status.BATCH_COMPLETED_AND_CONSUMED;
        }
    }
}
