package se.liu.ida.hefquin.engine.queryproc.impl.compiler;

import se.liu.ida.hefquin.base.queryplan.ExpectedVariables;
import se.liu.ida.hefquin.engine.queryplan.executable.*;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ExecPlanSamplingTaskBase;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ExecPlanTask;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.TaskBasedExecutablePlanImpl;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.pushbased.*;
import se.liu.ida.hefquin.engine.queryplan.physical.*;
import se.liu.ida.hefquin.engine.queryproc.ExecutionContext;
import se.liu.ida.hefquin.engine.queryproc.QueryProcContext;

import java.util.LinkedList;

public abstract class TaskBasedSamplingQueryPlanCompilerBase extends QueryPlanCompilerBase {
    public TaskBasedSamplingQueryPlanCompilerBase( final QueryProcContext ctxt ) {
        super(ctxt);
    }

    @Override
    public ExecutablePlan compile( final PhysicalPlan qep ) {
        final ExecutionContext execCtxt = createExecContext();
        final LinkedList<ExecPlanTask> tasks = createTasks(qep, execCtxt);
        return new TaskBasedExecutablePlanImpl(tasks, execCtxt);
    }

    protected LinkedList<ExecPlanTask> createTasks( final PhysicalPlan qep,
                                                    final ExecutionContext execCxt ) {
        final int preferredOutputBlockSize = 1;
        final LinkedList<ExecPlanTask> tasks = new LinkedList<>();
        createWorker().createTasks(qep, tasks, preferredOutputBlockSize, execCxt);
        return tasks;
    }

    protected Worker createWorker() {
        return new Worker();
    }

    protected class Worker
    {
        public void createTasks( final PhysicalPlan qep,
                                 final LinkedList<ExecPlanTask> tasks,
                                 final int preferredOutputBlockSize,
                                 final ExecutionContext execCxt) {
            final ExecPlanTask newTask = _createTasks(qep, tasks, preferredOutputBlockSize, execCxt);
            tasks.addFirst(newTask);
        }

        protected ExecPlanTask _createTasks(final PhysicalPlan qep,
                                            final LinkedList<ExecPlanTask> tasks,
                                            final int preferredOutputBlockSize,
                                            final ExecutionContext execCxt) {
            final PhysicalOperator pop = qep.getRootOperator();
            if ( pop instanceof NullaryPhysicalOp)
            {
                final NullaryExecutableOp execOp = (NullaryExecutableOp) pop.createExecOp(true);
                return createTaskForNullaryExecOp(execOp, execCxt, preferredOutputBlockSize);
            }
            else if ( pop instanceof UnaryPhysicalOp)
            {
                final PhysicalPlan subPlan = qep.getSubPlan(0);

                final UnaryExecutableOp execOp = (UnaryExecutableOp) pop.createExecOp( true, subPlan.getExpectedVariables() );

                createTasks( subPlan, tasks, execOp.preferredInputBlockSize(), execCxt );
                final ExecPlanTask childTask = tasks.getFirst();

                PushBasedExecPlanSamplingTaskForUnaryOperator returnTask = createTaskForUnaryExecOp(execOp, childTask, execCxt, preferredOutputBlockSize);
                try{
                    ((PushBasedExecPlanSamplingTaskBase) childTask).setUpper(returnTask);
                }catch (Exception e) {
                    System.out.println("couldn't cast ExecPlanTask to PushBasedExecPlanSamplingTaskBase");
                    e.printStackTrace(System.out);
                }
                return returnTask;
            }
            else if ( pop instanceof BinaryPhysicalOp)
            {
                final PhysicalPlan subPlan1 = qep.getSubPlan(0);
                final PhysicalPlan subPlan2 = qep.getSubPlan(1);

                final BinaryExecutableOp execOp = (BinaryExecutableOp) pop.createExecOp(
                        true,
                        subPlan1.getExpectedVariables(),
                        subPlan2.getExpectedVariables() );

                createTasks( subPlan1, tasks, execOp.preferredInputBlockSizeFromChild1(), execCxt);
                final ExecPlanTask childTask1 = tasks.getFirst();

                createTasks( subPlan2, tasks, execOp.preferredInputBlockSizeFromChild2(), execCxt );
                final ExecPlanTask childTask2 = tasks.getFirst();

                PushBasedExecPlanSamplingTaskForBinaryOperator returnTask = createTaskForBinaryExecOp(execOp, childTask1, childTask2, execCxt, preferredOutputBlockSize);
                try{
                    ((PushBasedExecPlanSamplingTaskBase) childTask1).setUpper(returnTask);
                    ((PushBasedExecPlanSamplingTaskBase) childTask2).setUpper(returnTask);
                }catch (Exception e) {
                    System.out.println("couldn't cast ExecPlanTask to PushBasedExecPlanSamplingTaskBase");
                    e.printStackTrace(System.out);
                }
                return returnTask;
            }
            else if ( pop instanceof NaryPhysicalOp )
            {
                final ExpectedVariables[] expVars = new ExpectedVariables[ qep.numberOfSubPlans() ];
                for ( int i = 0; i < expVars.length; i++ ) {
                    expVars[i] = qep.getSubPlan(i).getExpectedVariables();
                }

                final NaryExecutableOp execOp = (NaryExecutableOp) pop.createExecOp(true, expVars);

                final ExecPlanTask[] childTasks = new ExecPlanTask[ qep.numberOfSubPlans() ];
                for ( int i = 0; i < childTasks.length; i++ ) {
                    createTasks( qep.getSubPlan(i),
                            tasks,
                            execOp.preferredInputBlockSizeFromChilden(),
                            execCxt);

                    childTasks[i] = tasks.getFirst();
                }

                PushBasedExecPlanSamplingTaskForNaryOperator returnTask = createTaskForNaryExecOp(execOp, childTasks, execCxt, preferredOutputBlockSize);
                for (ExecPlanTask childTask : childTasks) {
                    try{
                        ((PushBasedExecPlanSamplingTaskBase) childTask).setUpper(returnTask);
                    }catch (Exception e) {
                        System.out.println("couldn't cast ExecPlanTask to PushBasedExecPlanSamplingTaskBase");
                        e.printStackTrace(System.out);
                    }
                }
                return returnTask;
            }
            else
            {
                throw new IllegalArgumentException();
            }
        }

    } // end of helper class Worker

    protected abstract PushBasedExecPlanSamplingTaskForNullaryOperator createTaskForNullaryExecOp(NullaryExecutableOp op,
                                                                                                  ExecutionContext execCxt,
                                                                                                  int preferredOutputBlockSize);

    protected abstract PushBasedExecPlanSamplingTaskForUnaryOperator createTaskForUnaryExecOp(UnaryExecutableOp op,
                                                                                              ExecPlanTask childTask,
                                                                                              ExecutionContext execCxt,
                                                                                              int preferredOutputBlockSize);

    protected abstract PushBasedExecPlanSamplingTaskForBinaryOperator createTaskForBinaryExecOp(BinaryExecutableOp op,
                                                                                                ExecPlanTask childTask1,
                                                                                                ExecPlanTask childTask2,
                                                                                                ExecutionContext execCxt,
                                                                                                int preferredOutputBlockSize);

    protected abstract PushBasedExecPlanSamplingTaskForNaryOperator createTaskForNaryExecOp(NaryExecutableOp op,
                                                                                            ExecPlanTask[] childTasks,
                                                                                            ExecutionContext execCxt,
                                                                                            int preferredOutputBlockSize);
}
