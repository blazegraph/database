/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Aug 20, 2010
 */

package com.bigdata.bop.ap;

import java.util.Properties;

import com.bigdata.bop.joinGraph.IEvaluationPlanFactory;
import com.bigdata.journal.IIndexManager;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.eval.AbstractJoinNexusFactory;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IRuleTaskFactory;

/**
 * Mock object.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MockJoinNexusFactory extends AbstractJoinNexusFactory {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public MockJoinNexusFactory(ActionEnum action, long writeTimestamp,
            long readTimestamp, Properties properties, int solutionFlags,
            IElementFilter<?> solutionFilter,
            IEvaluationPlanFactory evaluationPlanFactory,
            IRuleTaskFactory defaultRuleTaskFactory) {
        
        super(action, writeTimestamp, readTimestamp, properties, solutionFlags,
                solutionFilter, evaluationPlanFactory, defaultRuleTaskFactory);
        
    }

    @Override
    protected IJoinNexus newJoinNexus(IIndexManager indexManager) {
        return new MockJoinNexus(this,indexManager);
    }

}
