package se.liu.ida.hefquin.engine.queryplan.executable.impl.pushbased;

import se.liu.ida.hefquin.engine.queryplan.executable.ExecutableOperator;
import se.liu.ida.hefquin.engine.queryplan.executable.IntermediateResultElementSink;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ExecPlanTask;
import se.liu.ida.hefquin.engine.queryproc.ExecutionContext;

public class SamplingConnectorForAdditionalConsumer extends PushBasedExecPlanSamplingTaskBase implements IntermediateResultElementSink{
    protected SamplingConnectorForAdditionalConsumer( final ExecutionContext execCxt,
                                              final int preferredMinimumBlockSize ) {
        super(execCxt, preferredMinimumBlockSize);
    }

    public void setStatus( final PushBasedExecPlanSamplingTaskBase.Status newStatus ) {
        super.setStatus(newStatus);
    }

    @Override
    protected ExecutableOperator getExecOp() { throw new UnsupportedOperationException(); }

    @Override
    protected void produceOutput( final IntermediateResultElementSink sink ) { throw new UnsupportedOperationException(); }

    @Override
    protected void propagateNextBatch() {
        synchronized (availableResultBlocks){
            // we clear the queue to start off of a clean, new batch
            this.availableResultBlocks.clear();
            this.setStatus(Status.READY_NEXT_BATCH);
        }
    }

    @Override
    public ExecPlanTask addConnectorForAdditionalConsumer(final int preferredMinimumBlockSize ) { throw new UnsupportedOperationException(); }

    @Override
    public boolean isPreviousBatchDone() {
        return getStatus() == Status.BATCH_COMPLETED_AND_CONSUMED;
    }

    @Override
    public void clearAvailableBlocks() {
        synchronized (availableResultBlocks) {
            availableResultBlocks.clear();
        }
    }
}
