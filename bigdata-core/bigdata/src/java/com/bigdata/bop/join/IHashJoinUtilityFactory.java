/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General License for more details.

You should have received a copy of the GNU General License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
/*
 * Created on Aug 19, 2015
 */

package com.bigdata.bop.join;

import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpEvaluationContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.controller.INamedSolutionSetRef;

/**
 * Interface for the factory pattern to create a {@link IHashJoinUtility}.
 * 
 * @author <a href="http://olafhartig.de/">Olaf Hartig</a>
 * @version $Id$
 */
public interface IHashJoinUtilityFactory {
    
    /**
     * Return an instance of the {@link IHashJoinUtility}.
     * 
     * @param context
     *            The {@link BOpEvaluationContext}
     * @param namedSetRef
     *            Metadata to identify the named solution set.
     * @param op
     *            The operator whose annotation will inform the construction of
     *            the hash index. 
     * @param joinType
     *            The type of join.
     */
    IHashJoinUtility create(//
            BOpContext<IBindingSet> context,//
            INamedSolutionSetRef namedSetRef,//
            PipelineOp op,//
            JoinTypeEnum joinType//
            );
    
}
