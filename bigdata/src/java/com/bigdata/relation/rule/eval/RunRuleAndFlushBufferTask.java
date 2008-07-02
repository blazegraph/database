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
    
    /**
     * 
     */
    private static final long serialVersionUID = -2910127641227561854L;
    
    private final IStepTask stepTask;
    private final IBuffer buffer;
    
    public RunRuleAndFlushBufferTask(IStepTask stepTask, IBuffer buffer) {
        
        if (stepTask == null)
            throw new IllegalArgumentException();

        if (buffer == null)
            throw new IllegalArgumentException();
        
        this.stepTask = stepTask;
        
        this.buffer = buffer;
        
    }
    
    public RuleStats call() throws Exception {

        final RuleStats ruleStats = stepTask.call();
        
        if(log.isDebugEnabled()) {
            
            log.debug("Flushing buffer.");
            
        }
        
        final long mutationCount = buffer.flush();

        ruleStats.mutationCount.addAndGet(mutationCount);

        if(log.isDebugEnabled()) {
            
            log.debug("Flushed buffer: mutationCount=" + mutationCount
                    + ", stats=" + ruleStats);
            
        }

        return ruleStats;
        
    }
    
}