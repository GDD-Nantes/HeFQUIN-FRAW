package se.liu.ida.hefquin.engine.queryplan.executable.impl.pushbased;

import se.liu.ida.hefquin.engine.queryplan.executable.*;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ExecPlanTask;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ExecPlanTaskInputException;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ExecPlanTaskInterruptionException;
import se.liu.ida.hefquin.engine.queryproc.ExecutionContext;

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

//        if ( extraConnectors != null ) {
//            for ( final SamplingConnectorForAdditionalConsumer c : extraConnectors ) {
//                c.wrapUpBatch(failed, interrupted);
//            }
//        }

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
    public void propagateNextBatch() {
        synchronized (availableResultBlocks){
//            if(Objects.nonNull(this.extraConnectors))
//                this.extraConnectors.forEach(ec -> ec.propagateNextBatch());
            input.propagateNextBatch();

            // we clear the queue to start off of a clean, new batch
            this.availableResultBlocks.clear();
            this.setStatus(Status.READY_NEXT_BATCH);
        }
    }

    @Override
    public boolean isPreviousBatchDone() {
//        boolean extraConnectorsDone = Objects.isNull(extraConnectors) ? true : extraConnectors.stream().allMatch(ec -> ec.isPreviousBatchDone());
        synchronized (availableResultBlocks){
            if (getStatus() != Status.AVAILABLE) return false;
            return input.isPreviousBatchDone();
        }
//        return extraConnectorsDone && inputsDone && getStatus() == Status.AVAILABLE;
    }

    @Override
    public void clearAvailableBlocks() {
        synchronized (availableResultBlocks) {
            availableResultBlocks.clear();
            input.clearAvailableBlocks();
        }
    }

    @Override
    public void initializeFirstBatch() {
        synchronized (availableResultBlocks) {
            this.input.initializeFirstBatch();
            this.setStatus(Status.AVAILABLE);
        }
    }
}
