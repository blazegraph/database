package com.bigdata.relation.rule.eval;

import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IRuleTaskFactory;

/**
 * Factory class is used for sequential {@link IRule} step execution. It
 * wraps the selected {@link IStepTask} inside of a
 * {@link RunRuleAndFlushBufferTask} to ensure that the {@link IBuffer} on
 * which the {@link IStepTask} wrote its {@link ISolution}s gets
 * {@link IBuffer#flush()}ed after the {@link IStepTask} is executed.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @deprecated This is now handled by {@link MutationTask#newMutationTasks(com.bigdata.relation.rule.IStep, IJoinNexus, java.util.Map)}
 */
public class RunRuleAndFlushBufferTaskFactory implements IRuleTaskFactory {

    /**
     * 
     */
    private static final long serialVersionUID = 1396152962479786103L;
    
    private final IRuleTaskFactory delegate;
    
    public RunRuleAndFlushBufferTaskFactory(IRuleTaskFactory delegate) {

        if (delegate == null)
            throw new IllegalArgumentException();

        this.delegate = delegate;
        
    }

    public IStepTask newTask(IRule rule, IJoinNexus joinNexus,
            IBuffer<ISolution[]> buffer) {

        if (rule == null)
            throw new IllegalArgumentException();

        if (joinNexus == null)
            throw new IllegalArgumentException();

        if (buffer == null)
            throw new IllegalArgumentException();

        final IStepTask task = delegate.newTask(rule, joinNexus, buffer);

        return new RunRuleAndFlushBufferTask(task, buffer);
        
    }
    
}
