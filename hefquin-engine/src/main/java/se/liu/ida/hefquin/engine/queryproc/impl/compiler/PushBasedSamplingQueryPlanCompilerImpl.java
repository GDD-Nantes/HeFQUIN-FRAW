package se.liu.ida.hefquin.engine.queryproc.impl.compiler;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import se.liu.ida.hefquin.engine.queryplan.executable.*;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ExecPlanTask;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.TaskBasedExecutablePlanImpl;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.TaskBasedExecutableSamplingPlanImpl;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.pushbased.*;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalPlan;
import se.liu.ida.hefquin.engine.queryproc.ExecutionContext;
import se.liu.ida.hefquin.engine.queryproc.QueryProcContext;

public class PushBasedSamplingQueryPlanCompilerImpl extends TaskBasedSamplingQueryPlanCompilerBase
{
    public PushBasedSamplingQueryPlanCompilerImpl( final QueryProcContext ctxt ) {
        super(ctxt);
    }

    @Override
    public ExecutablePlan compile(PhysicalPlan qep) {
        final ExecutionContext execCtxt = createExecContext();
        final LinkedList<ExecPlanTask> tasks = createTasks(qep, execCtxt);
        return new TaskBasedExecutableSamplingPlanImpl(tasks, execCtxt);
    }

    @Override
    protected PushBasedExecPlanTask createTaskForNullaryExecOp( final NullaryExecutableOp op,
                                                                final ExecutionContext execCxt,
                                                                final int preferredOutputBlockSize ) {
        return new PushBasedExecPlanTaskForNullaryOperator(op, execCxt, preferredOutputBlockSize);
    }

    @Override
    protected PushBasedExecPlanTask createTaskForUnaryExecOp( final UnaryExecutableOp op,
                                                              final ExecPlanTask childTask,
                                                              final ExecutionContext execCxt,
                                                              final int preferredOutputBlockSize ) {
        return new PushBasedExecPlanTaskForUnaryOperator(op, childTask, execCxt, preferredOutputBlockSize);
    }

    @Override
    protected PushBasedExecPlanTask createTaskForBinaryExecOp( final BinaryExecutableOp op,
                                                               final ExecPlanTask childTask1,
                                                               final ExecPlanTask childTask2,
                                                               final ExecutionContext execCxt,
                                                               final int preferredOutputBlockSize ) {
        return new PushBasedExecPlanTaskForBinaryOperator(op, childTask1, childTask2, execCxt, preferredOutputBlockSize);
    }

    @Override
    protected PushBasedExecPlanTask createTaskForNaryExecOp( NaryExecutableOp op,
                                                             ExecPlanTask[] childTasks,
                                                             ExecutionContext execCxt,
                                                             int preferredOutputBlockSize) {
        // Is it better to cast or to use the impl in the signature ...?
        return new PushBasedExecPlanSamplingTaskForNaryOperator(op, childTasks, execCxt, preferredOutputBlockSize);
    }

    @Override
    protected LinkedList<ExecPlanTask> createTasks( final PhysicalPlan qep,
                                                    final ExecutionContext execCxt ) {
        final LinkedList<ExecPlanTask> tasks = super.createTasks(qep, execCxt);

        // remove all ConnectorForAdditionalConsumer from the list
        final LinkedList<ExecPlanTask> tasks2 = new LinkedList<>();
        for ( final ExecPlanTask t : tasks ) {
            if ( ! (t instanceof ConnectorForAdditionalConsumer) ) {
                tasks2.add(t);
            }
        }

        return tasks2;
    }

    @Override
    protected Worker createWorker() {
        return new Worker();
    }

    // makes sure that, for sub-plans that are contained multiple times in the
    // given physical plan, we create only one ExecPlanTask, which then pushes
    // its solution mappings to multiple consuming tasks
    protected class Worker extends TaskBasedSamplingQueryPlanCompilerBase.Worker
    {
        protected final Map<PhysicalPlan,ExecPlanTask> convertedSubPlans = new HashMap<>();

        @Override
        public void createTasks( final PhysicalPlan qep,
                                 final LinkedList<ExecPlanTask> tasks,
                                 final int preferredOutputBlockSize,
                                 final ExecutionContext execCxt) {
            final ExecPlanTask newTask;
            final ExecPlanTask probe = convertedSubPlans.get(qep);
            if ( probe != null ) {
                final PushBasedExecPlanTask t = (PushBasedExecPlanTask) probe;
                newTask = t.addConnectorForAdditionalConsumer(preferredOutputBlockSize);
            }
            else {
                newTask = _createTasks(qep, tasks, preferredOutputBlockSize, execCxt);
                convertedSubPlans.put(qep, newTask);
            }

            tasks.addFirst(newTask);
        }
    } // end of helper class Worker

}
