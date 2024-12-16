package se.liu.ida.hefquin.engine.queryplan.executable.impl.pushbased;

import se.liu.ida.hefquin.engine.queryplan.executable.*;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ExecPlanTask;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ExecPlanTaskInputException;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ExecPlanTaskInterruptionException;
import se.liu.ida.hefquin.engine.queryproc.ExecutionContext;

import java.util.Arrays;

public class PushBasedExecPlanSamplingTaskForUnaryOperator extends PushBasedExecPlanSamplingTaskBase{
    protected final UnaryExecutableOp op;
    protected final PushBasedExecPlanSamplingTaskBase input;

    public PushBasedExecPlanSamplingTaskForUnaryOperator( final UnaryExecutableOp op,
                                                  final ExecPlanTask input,
                                                  final ExecutionContext execCxt,
                                                  final int minimumBlockSize ) {
        super(execCxt, minimumBlockSize);

        assert op != null;
        assert input != null;

        this.op = op;
        // TODO : remove the casting !! inputs should be all PushBasedExecPlanSamplingTaskBase even though
        //  it's not clean
        this.input = (PushBasedExecPlanSamplingTaskBase) input;
    }

    @Override
    protected ExecutableOperator getExecOp() {
        return op;
    }

    @Override
    protected void produceOutput( final IntermediateResultElementSink sink )
            throws ExecOpExecutionException, ExecPlanTaskInputException, ExecPlanTaskInterruptionException {


        boolean failed       = false;
        boolean interrupted  = false;
        try {
            _produceOutput(sink);
        }
        catch ( final ExecOpExecutionException | ExecPlanTaskInputException e ) {
            setCauseOfFailure(e);
            failed = true;
        }
        catch ( final ExecPlanTaskInterruptionException  e ) {
            setCauseOfFailure(e);
            interrupted = true;
        }

        wrapUpBatch(failed, interrupted);

        if ( extraConnectors != null ) {
            for ( final ConnectorForAdditionalConsumer c : extraConnectors ) {
                c.wrapUp(failed, interrupted);
            }
        }


    }

    private void _produceOutput(IntermediateResultElementSink sink) throws ExecPlanTaskInputException, ExecPlanTaskInterruptionException, ExecOpExecutionException {
        boolean lastInputBlockConsumed = false;
        while ( ! lastInputBlockConsumed ) {
            final IntermediateResultBlock nextInputBlock = input.getNextIntermediateResultBlock();
            if ( nextInputBlock != null ) {
                op.process(nextInputBlock, sink, execCxt);
            }
            else {
                op.concludeExecution(sink, execCxt);
                lastInputBlockConsumed = true;
            }
        }
    }

    private void _produceOutputOneMapping(IntermediateResultElementSink sink) throws ExecPlanTaskInputException, ExecPlanTaskInterruptionException, ExecOpExecutionException {
        boolean lastInputBlockConsumed = false;
        while ( ! lastInputBlockConsumed ) {
            final IntermediateResultBlock nextInputBlock = input.getNextIntermediateResultBlock();
            if ( nextInputBlock != null ) {
                op.process(nextInputBlock, sink, execCxt);
                lastInputBlockConsumed = true;
            }
            else {
                op.concludeExecution(sink, execCxt);
                lastInputBlockConsumed = true;
            }
        }
    }

    @Override
    protected void propagateNextBatch() throws ExecPlanTaskInterruptionException, ExecPlanTaskInputException {
        synchronized (availableResultBlocks){
            input.propagateNextBatch();
            setStatus(Status.READY_NEXT_BATCH);
        }
    }

    @Override
    protected boolean isPreviousBatchDone() {
        synchronized (availableResultBlocks){
            boolean inputsDone = input.isPreviousBatchDone();
            return inputsDone && getStatus() == Status.BATCH_COMPLETED_AND_CONSUMED;
        }
    }
}
