package com.bigdata.relation.rule.eval;

import java.io.Serializable;

import org.apache.log4j.Logger;

import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.rule.IRule;

/**
 * Helper class is used for sequential {@link IRule} step execution. It runs
 * an {@link IStepTask} and then {@link IBuffer#flush()}s the
 * {@link IBuffer} on which the {@link IStepTask} wrote its
 * {@link ISolution}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RunRuleAndFlushBufferTask implements IStepTask, Serializable {

    protected static final Logger log = Logger.getLogger(RunRuleAndFlushBufferTask.class);
    
    protected static final boolean DEBUG = log.isDebugEnabled();
    
    /**
     * 
     */
    private static final long serialVersionUID = -2910127641227561854L;
    
    private final IStepTask stepTask;
    private final IBuffer<ISolution[]> buffer;
    
    /**
     * 
     * @param stepTask
     *            A task.
     * @param buffer
     *            A thread-safe buffer containing chunks of {@link ISolution}s
     *            computed by that task.
     */
    public RunRuleAndFlushBufferTask(final IStepTask stepTask,
            final IBuffer<ISolution[]> buffer) {
        
        if (stepTask == null)
            throw new IllegalArgumentException();

        if (buffer == null)
            throw new IllegalArgumentException();

        this.stepTask = stepTask;

        this.buffer = buffer;

    }

    public RuleStats call() throws Exception {

        // run the rule.
        final RuleStats ruleStats = stepTask.call();

        if (DEBUG) {

            log.debug("Flushing buffer: size=" + buffer.size() + ", class="
                    + buffer.getClass().getName());

        }
        
        final long mutationCount = buffer.flush();

//        if (!ruleStats.mutationCount.compareAndSet(0L, mutationCount)) {
//
//            /*
//             * Since buffer#flush() reports the total #of solutions flushed so
//             * far, the mutation count should only be set once the rule has
//             * completed its execution. Otherwise solutions will be double
//             * counted.
//             */
//            
//            throw new AssertionError("Already set: mutationCount="
//                    + ruleStats.mutationCount+", task="+stepTask);
//            
//        }

        if(DEBUG) {
            
            log.debug("Flushed buffer: mutationCount=" + mutationCount);
//            + ", stats=" + ruleStats);
            
        }

        return ruleStats;
        
    }
    
}
