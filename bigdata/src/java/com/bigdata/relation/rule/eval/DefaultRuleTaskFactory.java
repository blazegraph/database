package com.bigdata.relation.rule.eval;

import com.bigdata.journal.IIndexManager;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IRuleTaskFactory;
import com.bigdata.service.IBigdataFederation;

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

    public IStepTask newTask(final IRule rule, final IJoinNexus joinNexus,
            final IBuffer<ISolution[]> buffer) {

        final IIndexManager indexManager = joinNexus.getIndexManager();
        
        if (indexManager instanceof IBigdataFederation) {
            
            final IBigdataFederation fed = (IBigdataFederation)indexManager;
            
            if(fed.isScaleOut()) {

                // scale-out join.
                return new JoinMasterTask(rule, joinNexus, buffer);

            }
            
        }
        
        // monolithic index join.
        return new NestedSubqueryWithJoinThreadsTask(rule, joinNexus, buffer);

    }

}
