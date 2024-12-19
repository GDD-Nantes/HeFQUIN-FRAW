package se.liu.ida.hefquin.engine.queryplan.executable.impl.pushbased;

import se.liu.ida.hefquin.base.data.SolutionMapping;
import se.liu.ida.hefquin.base.data.impl.SolutionMappingImpl;
import se.liu.ida.hefquin.base.utils.StatsPrinter;
import se.liu.ida.hefquin.engine.queryplan.executable.*;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.*;
import se.liu.ida.hefquin.engine.queryproc.ExecutionContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.Lock;

public abstract class PushBasedExecPlanSamplingTaskBase extends ExecPlanSamplingTaskBase
        implements PushBasedExecPlanTask, IntermediateResultElementSink {

    protected static final int DEFAULT_OUTPUT_BLOCK_SIZE = 1;

    protected final int outputBlockSize;

    private final IntermediateResultBlockBuilder blockBuilder = new GenericIntermediateResultBlockBuilderImpl();

    protected List<SamplingConnectorForAdditionalConsumer> extraConnectors = null;

    protected PushBasedExecPlanSamplingTaskBase upper;

    // debugging purpose only
    protected int attemptedRandomWalks = 0;

    private boolean producedEnoughWalks = false;

    public Object lock = new Object();

    protected PushBasedExecPlanSamplingTaskBase( final ExecutionContext execCxt, final int preferredMinimumBlockSize ) {
        super(execCxt, preferredMinimumBlockSize);

        this.upper = null;

        // ??? why can't a block be a size of one mapping ???
        if ( preferredMinimumBlockSize == 1 )
            outputBlockSize = DEFAULT_OUTPUT_BLOCK_SIZE;
        else
            outputBlockSize = preferredMinimumBlockSize;

        // Make it so blocks can be the size of one mapping?
//        this.outputBlockSize = preferredMinimumBlockSize;
    }

    @Override
    public ExecPlanTask addConnectorForAdditionalConsumer( final int preferredMinimumBlockSize ) {
        throw new UnsupportedOperationException("No additional consumers for sampling plans !");

//        if ( extraConnectors == null ) {
//            extraConnectors = new ArrayList<>();
//        }
//
//        final SamplingConnectorForAdditionalConsumer c = new SamplingConnectorForAdditionalConsumer(execCxt, preferredMinimumBlockSize);
//        extraConnectors.add(c);
//        return c;
    }

    @Override
    public final void run() {
        try {
//            if ( extraConnectors != null ) {
//                for ( final SamplingConnectorForAdditionalConsumer c : extraConnectors ) {
//                     c.setStatus(Status.RUNNING);
//                }
//            }

            final IntermediateResultElementSink sink = this;

//            produceOutput(sink);



            while( ! ( shouldStop() ) ){
                if(isReadyForNextBatch()){
                    setStatus(Status.RUNNING);
                    produceOutput(sink);
                }else {
                    synchronized (lock){
                        lock.wait();
                    }
                }
            }

//            initializeFirstBatch();

//            while ( !shouldStop() ){
//                if (isRoot()) {
//                    while(!isPreviousBatchDone()) {}
//                    propagateNextBatch();
//                } else {
//                }
//                produceOutput(sink);
//            }

            wrapUpTask();
        }
        catch ( final Throwable th ) {
            System.err.println("Unexpected exception in one of the ExecPlanTasks.");
            System.err.println( "--> The class of the executable operator of this ExecPlanTask is:" + getExecOp().getClass().getName() );
            System.err.println( "--> The current runtime statistics of this ExecPlanTask are:");
            StatsPrinter.print( getStats(), System.err, true ); // true=recursive
            System.err.println( "--> The stack trace of the exception that was caught is:");
            th.printStackTrace( System.err );
            System.err.println();
        }
    }

    public boolean shouldStop(){
        // for root operator : timeout or enough results
        // for not root operators : upper operator is completed
        if(isRoot())
            return producedEnoughWalks || false; // !timeout && !enough results TODO : timeout
        else
            return upper.shouldStop();
    }

    @Override
    public void send( final SolutionMapping element ) {
        synchronized (availableResultBlocks) {
            blockBuilder.add(element);

            // If we have collected enough solution mappings, produce the next
            // output result block with these solution mappings and inform the
            // consuming thread in case it is already waiting for the next block
            if ( blockBuilder.sizeOfCurrentBlock() >= outputBlockSize ) {
                final IntermediateResultBlock nextBlock = blockBuilder.finishCurrentBlock();
                availableResultBlocks.add(nextBlock);
                attemptedRandomWalks += nextBlock.size();
                availableResultBlocks.notify();
            }
        }

//        if ( extraConnectors != null ) {
//            for ( final SamplingConnectorForAdditionalConsumer c : extraConnectors ) {
//                c.send(element);
//            }
//        }
    }

    protected void wrapUpBatch(final boolean failed, final boolean interrupted )
    {
        synchronized (availableResultBlocks) {
            if ( failed ) {
                setStatus(Status.FAILED);
                availableResultBlocks.notifyAll();
            }
            else if ( interrupted ) {
                setStatus(Status.INTERRUPTED);
                availableResultBlocks.notifyAll();
            }
            else {
                // everything went well; let's see whether we still have some
                // output solution mappings for a final output result block
                if ( blockBuilder.sizeOfCurrentBlock() > 0 ) {
                    // yes we have; let's create the output result block and
                    // notify the potentially waiting consuming thread of it
                    attemptedRandomWalks += blockBuilder.sizeOfCurrentBlock();
                    availableResultBlocks.add( blockBuilder.finishCurrentBlock() );
                    setStatus(Status.BATCH_COMPLETED_NOT_CONSUMED);
                }
                else {
                    // no more output solution mappings; set the completion
                    // status depending on whether there still are output
                    // result blocks available to be consumed
                    if ( availableResultBlocks.isEmpty() )
                        setStatus(Status.AVAILABLE);
                    else
                        setStatus(Status.BATCH_COMPLETED_NOT_CONSUMED);
                }
                availableResultBlocks.notify();
            }
        }
    }

    protected void wrapUpTask( )
    {
        synchronized (availableResultBlocks) {
            if ( this.getStatus() == Status.FAILED || this.getStatus() == Status.INTERRUPTED ) {
                // do nothing, wrapUpBatch already set the statuses and there is nothing else to do
            }
            else {
                // everything went well; let's see whether we still have some
                // output solution mappings for a final output result block
                if ( blockBuilder.sizeOfCurrentBlock() > 0 ) {
                    // yes we have; let's create the output result block and
                    // notify the potentially waiting consuming thread of it
                    attemptedRandomWalks += blockBuilder.sizeOfCurrentBlock();
                    availableResultBlocks.add( blockBuilder.finishCurrentBlock() );
                    setStatus(Status.TASK_COMPLETED_NOT_CONSUMED);
                }
                else {
                    // no more output solution mappings; set the completion
                    // status depending on whether there still are output
                    // result blocks available to be consumed
                    if ( availableResultBlocks.isEmpty() )
                        setStatus(Status.FINISHED);
                    else {
                        setStatus(Status.TASK_COMPLETED_NOT_CONSUMED);
                    }
                }
                availableResultBlocks.notify();
            }
        }
    }


    @Override
    public final IntermediateResultBlock getNextIntermediateResultBlock()
            throws ExecPlanTaskInterruptionException, ExecPlanTaskInputException {

        synchronized (availableResultBlocks) {

            IntermediateResultBlock nextBlock = null;
            while ( nextBlock == null && getStatus() != Status.AVAILABLE && getStatus() != Status.FINISHED) {
                // before trying to get the next block, make sure that
                // we have not already reached some problematic status
                final Status currentStatus = getStatus();
                if ( currentStatus == Status.FAILED ) {
                    throw new ExecPlanTaskInputException("Execution of this task has failed with an exception (operator: " + getExecOp().toString() + ").", getCauseOfFailure() );
                }
                else if ( currentStatus == Status.INTERRUPTED ) {
                    throw new ExecPlanTaskInputException("Execution of this task has been interrupted (operator: " + getExecOp().toString() + ").");
                }
                else if ( currentStatus == Status.WAITING_TO_BE_STARTED ) {
                    // this status is not actually a problem; the code in the
                    // remainder of this while loop will simply start waiting
                    // for the first result block to become available (which
                    // should happen after the execution of this task will
                    // get started eventually)
                    //System.err.println("Execution of this task has not started yet (operator: " + getExecOp().toString() + ").");
                }

                // try to get the next block
                nextBlock = availableResultBlocks.poll();


                // Did we reach the end of all result blocks to be expected?
                if ( currentStatus == Status.BATCH_COMPLETED_NOT_CONSUMED && availableResultBlocks.isEmpty() ) {
                    setStatus(Status.AVAILABLE); // yes, we did
                }

                // Did we reach the end of all result blocks to be expected?
                if ( currentStatus == Status.TASK_COMPLETED_NOT_CONSUMED && availableResultBlocks.isEmpty() ) {
                    setStatus(Status.FINISHED); // yes, we did
                }

                if ( nextBlock == null
                        && getStatus() != Status.AVAILABLE
                        && getStatus() != Status.FINISHED ) {
                    // if no next block was available and the producing
                    // thread is still active, wait for the notification
                    // that a new block has become available
                    try {
                        availableResultBlocks.wait();
                    } catch ( final InterruptedException e ) {
                        throw new ExecPlanTaskInterruptionException(e);
                    }
                }
            }


            if( isRoot() && !producedEnoughWalks && nextBlock == null ){
                // only works if a batch is always one mapping only
                blockBuilder.add(new SolutionMappingImpl());
                nextBlock = blockBuilder.finishCurrentBlock();
                attemptedRandomWalks += nextBlock.size();
            }

            return nextBlock;
        }
    }

    protected boolean isReadyForNextBatch(){
        return getStatus() == Status.READY_NEXT_BATCH;
    }

    protected boolean isRoot(){
        return this.upper == null;
    }

    public void setUpper( PushBasedExecPlanSamplingTaskBase upper){
        this.upper = upper;
    }

    public void enoughWalks(){
        this.producedEnoughWalks = true;
    }

    public abstract void propagateNextBatch();

    public abstract boolean isPreviousBatchDone();

    protected abstract void produceOutput( final IntermediateResultElementSink sink )
            throws ExecOpExecutionException, ExecPlanTaskInputException, ExecPlanTaskInterruptionException;

    public abstract void clearAvailableBlocks();

    public abstract void initializeFirstBatch();
}
