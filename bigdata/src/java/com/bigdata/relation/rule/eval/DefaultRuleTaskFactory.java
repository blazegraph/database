package com.bigdata.relation.rule.eval;

import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IRuleTaskFactory;

/**
 * Default factory for tasks to execute {@link IRule}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DefaultRuleTaskFactory implements IRuleTaskFactory {

    /**
     * 
     */
    private static final long serialVersionUID = -6751546625682021618L;

    public IStepTask newTask(IRule rule, IJoinNexus joinNexus,
            IBuffer<ISolution[]> buffer) {

        return new NestedSubqueryWithJoinThreadsTask(rule, joinNexus, buffer);

    }

}
