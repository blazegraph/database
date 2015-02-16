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
 * Created on Aug 18, 2010
 */

package com.bigdata.bop.join;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.NV;
import com.bigdata.bop.controller.INamedSolutionSetRef;

/**
 * Concrete implementation based on the {@link HTreeHashJoinUtility}.
 * 
 * @see HTreeSolutionSetHashJoinOp
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class HTreeHashIndexOp extends HashIndexOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends HashIndexOp.Annotations,
            HTreeHashJoinAnnotations {

    }
    
    /**
     * Deep copy constructor.
     */
    public HTreeHashIndexOp(final HTreeHashIndexOp op) {
        super(op);
    }
    
    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    public HTreeHashIndexOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

    }

    public HTreeHashIndexOp(final BOp[] args, final NV... annotations) {

        this(args, NV.asMap(annotations));

    }

    @Override
    protected HTreeHashJoinUtility newState(
            final BOpContext<IBindingSet> context,
            final INamedSolutionSetRef namedSetRef, final JoinTypeEnum joinType) {

        return new HTreeHashJoinUtility(context.getMemoryManager(namedSetRef
                .getQueryId()), this, joinType);

    }

}
