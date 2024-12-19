package se.liu.ida.hefquin.engine.queryplan.executable.impl.pushbased;

import se.liu.ida.hefquin.base.data.SolutionMapping;
import se.liu.ida.hefquin.engine.queryplan.executable.*;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.*;
import se.liu.ida.hefquin.engine.queryproc.ExecutionContext;

import java.util.*;

public class PushBasedExecPlanSamplingTaskForNaryOperator extends PushBasedExecPlanSamplingTaskBase {

    protected final NaryExecutableOp op;
    protected final PushBasedExecPlanSamplingTaskBase[] inputs;

    protected final Random rand;
    protected int chosen;

    // Currently, should only be used for multiwayunion, not multiwayjoin, since every batch, a single child is
    // chosen to produce output.
    public PushBasedExecPlanSamplingTaskForNaryOperator( final NaryExecutableOp op,
                                                 final ExecPlanTask[] inputs,
                                                 final ExecutionContext execCxt,
                                                 final int minimumBlockSize) {
        super(execCxt, minimumBlockSize);

        assert op != null;
        assert inputs != null;
        assert inputs.length > 0;

        rand = new Random();

        this.op = op;
        // TODO : not pretty
        this.inputs = Arrays.copyOf(inputs, inputs.length, PushBasedExecPlanSamplingTaskBase[].class);
    }

    @Override
    protected void produceOutput(IntermediateResultElementSink sink) throws ExecOpExecutionException, ExecPlanTaskInputException, ExecPlanTaskInterruptionException {

        boolean failed       = false;
        boolean interrupted  = false;

        try {
            produceOutputFromRandomInput(sink);
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

    protected void produceOutputFromRandomInput( final IntermediateResultElementSink sink )
            throws ExecOpExecutionException, ExecPlanTaskInputException, ExecPlanTaskInterruptionException {
        boolean lastInputBlockConsumed = false;
        while ( ! lastInputBlockConsumed ) {
            final IntermediateResultBlock nextInputBlock = inputs[chosen].getNextIntermediateResultBlock();
            if ( nextInputBlock != null ) {
                op.processBlockFromXthChild(chosen, nextInputBlock, sink, execCxt);
            }
            else {
                op.wrapUpForXthChild(chosen, sink, execCxt);
                lastInputBlockConsumed = true;
            }
        }
    }

    protected void produceOutputFromRandomInputOneMapping( final IntermediateResultElementSink sink )
            throws ExecOpExecutionException, ExecPlanTaskInputException, ExecPlanTaskInterruptionException {
        boolean lastInputBlockConsumed = false;
        while ( ! lastInputBlockConsumed ) {
            final IntermediateResultBlock nextInputBlock = inputs[chosen].getNextIntermediateResultBlock();
            if ( nextInputBlock != null ) {
                op.processBlockFromXthChild(chosen, nextInputBlock, sink, execCxt);
                lastInputBlockConsumed = true;
            }
            else {
                op.wrapUpForXthChild(chosen, sink, execCxt);
                lastInputBlockConsumed = true;
            }
        }

        // We simulate consuming all inputs, even though we only consumed one
        Arrays.stream(inputs).forEach(i -> {
            i.setStatus(Status.AVAILABLE);
            i.clearAvailableBlocks();
        });
    }

    protected void produceAllOutputByConsumingAllInputsInParallelOneMapping( final IntermediateResultElementSink sink ) throws ExecPlanTaskInputException, ExecOpExecutionException, ExecPlanTaskInterruptionException {

        // optimization of produceOneOutputFromRandomInput ?
        CollectingIntermediateResultElementSink tempSink = new CollectingIntermediateResultElementSink();

        produceOutputByConsumingAllInputsInParallel(tempSink);

        // in the implementation, getCollectedSolutionMappings always return an ArrayList, so this cast shouldn't be an
        // issue. Still, it would be better to have a dedicated sink that returns specifically a List
        // TODO :
        List<SolutionMapping> tempResults = (ArrayList) tempSink.getCollectedSolutionMappings();

        if (!tempResults.isEmpty()) sink.send( tempResults.get( rand.nextInt( tempResults.size() ) ) );
    }

    /**
     * Consumes the complete i-th input first (and pushes that input to the
     * operator {@link #op}), before moving on to the (i+1)-th input.
     */
    protected void produceOutputByConsumingAllInputsInParallel( final IntermediateResultElementSink sink )
            throws ExecOpExecutionException, ExecPlanTaskInputException, ExecPlanTaskInterruptionException {

        final boolean[] inputConsumed = new boolean[inputs.length];
        for ( int i = 0; i < inputs.length; i++ ) { inputConsumed[i] = false; }

        int indexOfNextInputToWaitFor = 0;
        int numberOfInputsConsumed = 0;
        while ( numberOfInputsConsumed < inputs.length ) {
            // Before blindly asking any of the inputs to give us its next
            // IntermediateResultBlock (which may cause this thread to wait
            // if no such block is available at the moment), let's first ask
            // them if they currently have a block available. If so, request
            // the next block from the input that says it has a block available.
            boolean blockConsumed = false;
            for ( int i = 0; i < inputs.length; i++ ) {
                if ( ! inputConsumed[i] && inputs[i].hasNextIntermediateResultBlockAvailable() ) {
                    // calling 'getNextIntermediateResultBlock()' should not cause this thread to wait
                    final IntermediateResultBlock nextInputBlock = inputs[i].getNextIntermediateResultBlock();
                    if ( nextInputBlock != null ) {
                        op.processBlockFromXthChild(i, nextInputBlock, sink, execCxt);
                    }

                    blockConsumed = true;
                }
            }

            if ( ! blockConsumed ) {
                // If none of the inputs had a block available at the moment,
                // we ask one of them to produce its next block, which may
                // cause this thread to wait until that next block has been
                // produced. To decide which of the inputs we ask (and, then,
                // wait for) we use a round robin approach. To this end, we
                // use the 'indexOfNextInputToWaitFor' pointer which we advance
                // each time we leave this code block here.

                // First, we have to make sure that 'indexOfNextInputToWaitFor'
                // points to an input that has not been consumed completely yet.
                while ( inputConsumed[indexOfNextInputToWaitFor] == true ) {
                    indexOfNextInputToWaitFor = advanceIndexOfInput(indexOfNextInputToWaitFor);
                }

                // Now we ask that input to produce its next block, which may
                // cause this thread to wait.
                final IntermediateResultBlock nextInputBlock = inputs[indexOfNextInputToWaitFor].getNextIntermediateResultBlock();
                if ( nextInputBlock != null ) {
                    op.processBlockFromXthChild(indexOfNextInputToWaitFor, nextInputBlock, sink, execCxt);
                }
                else {
                    op.wrapUpForXthChild(indexOfNextInputToWaitFor, sink, execCxt);
                    inputConsumed[indexOfNextInputToWaitFor] = true;
                    numberOfInputsConsumed++;
                }

                // Finally, we advance the 'indexOfNextInputToWaitFor' pointer
                // so that, next time we will have to wait, we will wait for
                // the next input (rather than always waiting for the same
                // input before moving on to the next input).
                indexOfNextInputToWaitFor = advanceIndexOfInput(indexOfNextInputToWaitFor);
            }
        }
    }

    /**
     * Returns the given integer increased by one, unless such an
     * increase results in an integer that is outside of the bounds
     * of the {@link #inputs} array, in which case the function returns
     * zero (effectively jumping back to the first index in the array).
     */
    protected int advanceIndexOfInput( final int currentIndex ) {
        final int i = currentIndex + 1;
        return ( i < inputs.length ) ? i : 0;
    }

    @Override
    protected ExecutableOperator getExecOp() {
        return op;
    }



    @Override
    public void propagateNextBatch() {
        synchronized (availableResultBlocks){
//            if(Objects.nonNull(this.extraConnectors))
//                this.extraConnectors.forEach(ec -> ec.propagateNextBatch());

            // Let's only start a task if it's going to be used in the next batch. That is, we pick the branch of the
            // union that we're going to use for the next batch here

            chosen = rand.nextInt(this.inputs.length);
            PushBasedExecPlanSamplingTaskBase chosenInput = this.inputs[chosen];

            chosenInput.propagateNextBatch();

            //Arrays.stream(this.inputs).forEach(i -> i.propagateNextBatch());
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
            return inputs[chosen].isPreviousBatchDone();
        }
//        return extraConnectorsDone && inputsDone && getStatus() == Status.AVAILABLE;
    }

    @Override
    public void clearAvailableBlocks() {
        synchronized (availableResultBlocks) {
            availableResultBlocks.clear();
            inputs[chosen].clearAvailableBlocks();
        }
    }

    @Override
    public void initializeFirstBatch() {
        synchronized (availableResultBlocks) {
            for (PushBasedExecPlanSamplingTaskBase task : inputs){
                task.initializeFirstBatch();
            }
            this.setStatus(Status.AVAILABLE);
        }
    }

}
