/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jul 1, 2008
 */

package com.bigdata.relation.rule;

import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IRuleTask;
import com.bigdata.relation.rule.eval.ISolution;

/**
 * An interface providing a factory for {@link IRuleTask}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IRuleTaskFactory {

    /**
     * An optional {@link IRuleTask} that will be used to evaluate the rule for
     * the {@link IRule} in place of the default evaluation strategy (optional)
     * 
     * @param rule
     *            The rule (MAY have been specialized since it was declared).
     * @param joinNexus
     *            Encapsulates various important information required for join
     *            operations.
     * @param buffer
     *            The buffer onto which the computed {@link ISolution}s for the
     *            {@link IRule} must be written.
     * 
     * @return <code>null</code> unless custom evaluation is desired.
     */
    public IRuleTask newTask(IRule rule, IJoinNexus joinNexus, IBuffer<ISolution> buffer);

}
