package se.liu.ida.hefquin.engine.queryplan.executable.impl.pushbased;

import se.liu.ida.hefquin.engine.queryplan.executable.*;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ExecPlanTask;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ExecPlanTaskInputException;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ExecPlanTaskInterruptionException;
import se.liu.ida.hefquin.engine.queryproc.ExecutionContext;

public class PushBasedExecPlanSamplingTaskForBinaryOperator extends PushBasedExecPlanSamplingTaskBase {

    protected final BinaryExecutableOp op;
    protected final PushBasedExecPlanSamplingTaskBase input1;
    protected final PushBasedExecPlanSamplingTaskBase input2;

    public PushBasedExecPlanSamplingTaskForBinaryOperator( final BinaryExecutableOp op,
                                                   final ExecPlanTask input1,
                                                   final ExecPlanTask input2,
                                                   final ExecutionContext execCxt,
                                                   final int minimumBlockSize) {
        super(execCxt, minimumBlockSize);

        assert op != null;
        assert input1 != null;
        assert input2 != null;

        this.op = op;

        // TODO : remove casting
        this.input1 = (PushBasedExecPlanSamplingTaskBase) input1;
        this.input2 = (PushBasedExecPlanSamplingTaskBase) input2;
    }

    @Override
    protected ExecutableOperator getExecOp() {
        return op;
    }

    @Override
    protected void produceOutput( final IntermediateResultElementSink sink )
            throws ExecOpExecutionException, ExecPlanTaskInputException, ExecPlanTaskInterruptionException {

        boolean failed = false;
        boolean interrupted = false;

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

//        if ( extraConnectors != null ) {
//            for ( final SamplingConnectorForAdditionalConsumer c : extraConnectors ) {
//                c.wrapUpBatch(failed, interrupted);
//            }
//        }

        wrapUpBatch(failed, interrupted);
    }

    private void _produceOutput(IntermediateResultElementSink sink) throws ExecPlanTaskInputException, ExecOpExecutionException, ExecPlanTaskInterruptionException {
        if ( op.requiresCompleteChild1InputFirst() )
            produceOutputByConsumingInput1First(sink);
        else
            //produceOutputByConsumingBothInputsInParallel(sink);
            produceOutputByConsumingInput1First(sink);
    }

    /**
     * Consumes the complete child 1 input first (and pushes that input to the
     * operator {@link #op}), before moving on to the input from child 2.
     */
    protected void produceOutputByConsumingInput1First( final IntermediateResultElementSink sink )
            throws ExecOpExecutionException, ExecPlanTaskInputException, ExecPlanTaskInterruptionException {

        boolean input1Consumed = false;
        while ( ! input1Consumed ) {
            final IntermediateResultBlock nextInputBlock = input1.getNextIntermediateResultBlock();
            if ( nextInputBlock != null ) {
                op.processBlockFromChild1(nextInputBlock, sink, execCxt);
            }
            else {
                op.wrapUpForChild1(sink, execCxt);
                input1Consumed = true;
            }
        }

        boolean input2Consumed = false;
        while ( ! input2Consumed ) {
            final IntermediateResultBlock nextInputBlock = input2.getNextIntermediateResultBlock();
            if ( nextInputBlock != null ) {
                op.processBlockFromChild2(nextInputBlock, sink, execCxt);
            }
            else {
                op.wrapUpForChild2(sink, execCxt);
                input2Consumed = true;
            }
        }
    }

    /**
     * Aims to consume both inputs in parallel.
     */
    protected void produceOutputByConsumingBothInputsInParallel( final IntermediateResultElementSink sink )
            throws ExecOpExecutionException, ExecPlanTaskInputException, ExecPlanTaskInterruptionException {

        boolean nextWaitForInput1 = true; // flag to switch between waiting for input 1 versus input 2
        boolean input1Consumed = false;
        boolean input2Consumed = false;
        while ( ! input1Consumed || ! input2Consumed ) {
            // Before blindly asking any of the two inputs to give us its next
            // IntermediateResultBlock (which may cause this thread to wait if
            // no such block is available at the moment), let's first ask them
            // if they currently have a block available. If so, request the next
            // block from the input that says it has a block available.
            boolean blockConsumed = false;
            if ( ! input1Consumed && input1.hasNextIntermediateResultBlockAvailable() )
            {
                // calling 'getNextIntermediateResultBlock()' should not cause this thread to wait
                final IntermediateResultBlock nextInputBlock = input1.getNextIntermediateResultBlock();
                if ( nextInputBlock != null ) {
                    op.processBlockFromChild1(nextInputBlock, sink, execCxt);
                }

                blockConsumed = true;
            }

            if ( ! input2Consumed && input2.hasNextIntermediateResultBlockAvailable() )
            {
                // calling 'getNextIntermediateResultBlock()' should not cause this thread to wait
                final IntermediateResultBlock nextInputBlock = input2.getNextIntermediateResultBlock();
                if ( nextInputBlock != null ) {
                    op.processBlockFromChild2(nextInputBlock, sink, execCxt);
                }

                blockConsumed = true;
            }

            if ( ! blockConsumed ) {
                // If none of the two inputs had a block available at the
                // moment, we ask one of them to produce its next block,
                // which may cause this thread to wait until that next
                // block has been produced. To decide which of the two
                // inputs we ask (and, then, wait for) we use a round
                // robin approach (i.e., always switch between the two
                // inputs). To this end, we use the 'nextWaitForInput1'
                // flag: if that flag is true, we will next ask (and wait
                // for) input 1; if that flag is false, we will next ask
                // (and wait for) input 2.
                if  ( nextWaitForInput1 && ! input1Consumed ) {
                    // calling 'getNextIntermediateResultBlock()' may cause this thread to wait
                    final IntermediateResultBlock nextInputBlock = input1.getNextIntermediateResultBlock();
                    if ( nextInputBlock != null ) {
                        op.processBlockFromChild1(nextInputBlock, sink, execCxt);
                    }
                    else {
                        op.wrapUpForChild1(sink, execCxt);
                        input1Consumed = true;
                    }
                }
                else if ( ! input2Consumed ) {
                    // calling 'getNextIntermediateResultBlock()' may cause this thread to wait
                    final IntermediateResultBlock nextInputBlock = input2.getNextIntermediateResultBlock();
                    if ( nextInputBlock != null ) {
                        op.processBlockFromChild2(nextInputBlock, sink, execCxt);
                    }
                    else {
                        op.wrapUpForChild2(sink, execCxt);
                        input2Consumed = true;
                    }
                }
                // flip the 'nextWaitForInput1' flag so that, next time we
                // have to wait, we will wait for the respective other input
                nextWaitForInput1 = ! nextWaitForInput1;
            }
        }
    }

    @Override
    public boolean isPreviousBatchDone() {
//        boolean extraConnectorsDone = Objects.isNull(extraConnectors) ? true : extraConnectors.stream().allMatch(ec -> ec.isPreviousBatchDone());
        synchronized (availableResultBlocks) {
            if (getStatus() != Status.AVAILABLE) return false;
            return input2.isPreviousBatchDone() && input1.isPreviousBatchDone();
        }
//        return inputsDone && extraConnectorsDone && getStatus() == Status.AVAILABLE;
    }

    @Override
    public void clearAvailableBlocks() {
        synchronized (availableResultBlocks) {
            availableResultBlocks.clear();
            input1.clearAvailableBlocks();
            input2.clearAvailableBlocks();
        }
    }

    @Override
    public void initializeFirstBatch() {
        synchronized (availableResultBlocks) {
            this.input1.initializeFirstBatch();
            this.input2.initializeFirstBatch();
            this.setStatus(Status.AVAILABLE);
        }
    }

    @Override
    public void propagateNextBatch() {
        synchronized (availableResultBlocks){
//            if(Objects.nonNull(this.extraConnectors))
//                this.extraConnectors.forEach(ec -> ec.propagateNextBatch());
            this.input1.propagateNextBatch();
            this.input2.propagateNextBatch();
            // we clear the queue to start off of a clean, new batch
            this.availableResultBlocks.clear();

            this.setStatus(Status.READY_NEXT_BATCH);
        }
    }
}
