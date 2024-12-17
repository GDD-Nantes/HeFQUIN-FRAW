package se.liu.ida.hefquin.engine.queryplan.executable.impl.pushbased;

import se.liu.ida.hefquin.base.data.SolutionMapping;
import se.liu.ida.hefquin.engine.queryplan.executable.ExecOpExecutionException;
import se.liu.ida.hefquin.engine.queryplan.executable.ExecutableOperator;
import se.liu.ida.hefquin.engine.queryplan.executable.IntermediateResultElementSink;
import se.liu.ida.hefquin.engine.queryplan.executable.NullaryExecutableOp;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.CollectingIntermediateResultElementSink;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ExecPlanTaskInputException;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ExecPlanTaskInterruptionException;
import se.liu.ida.hefquin.engine.queryproc.ExecutionContext;

import java.util.*;

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
//            op.execute(sink, execCxt);
            produceOutputOneMapping(sink);
        }
        catch ( final ExecOpExecutionException e ) {
            setCauseOfFailure(e);
            failed = true;
        }

        wrapUpBatch(failed, false);
//
//        if ( extraConnectors != null ) {
//            for ( final SamplingConnectorForAdditionalConsumer c : extraConnectors ) {
//                c.wrapUpBatch(failed, false);
//            }
//        }
    }

    protected void produceOutputOneMapping( final IntermediateResultElementSink sink ) throws ExecOpExecutionException {
        Random rand = new Random();

        CollectingIntermediateResultElementSink tempSink = new CollectingIntermediateResultElementSink();
        op.execute(tempSink, execCxt);
        List<SolutionMapping> tempResults = (ArrayList) tempSink.getCollectedSolutionMappings();

        // in the implementation, getCollectedSolutionMappings always return an ArrayList, so this cast shouldn't be an
        // issue. Still, it would be better to have a dedicated sink that returns specifically a List
        // TODO :
        if ( !tempResults.isEmpty() ) sink.send( tempResults.get( rand.nextInt( tempResults.size() ) ) );
    }

    @Override
    protected void propagateNextBatch() {
        synchronized (availableResultBlocks){
//            if(Objects.nonNull(this.extraConnectors))
//                this.extraConnectors.forEach(ec -> ec.propagateNextBatch());
            // we clear the queue to start off of a clean, new batch
            this.availableResultBlocks.clear();
            this.setStatus(Status.READY_NEXT_BATCH);
        }
    }

    @Override
    public boolean isPreviousBatchDone() {
//        boolean extraConnectorsDone = Objects.isNull(extraConnectors) ? true : extraConnectors.stream().allMatch(ec -> ec.isPreviousBatchDone());
        return getStatus() == Status.BATCH_COMPLETED_AND_CONSUMED;
//        return extraConnectorsDone && getStatus() == Status.BATCH_COMPLETED_AND_CONSUMED;
    }

    @Override
    public void clearAvailableBlocks() {
        synchronized (availableResultBlocks) {
            availableResultBlocks.clear();
        }
    }
}
