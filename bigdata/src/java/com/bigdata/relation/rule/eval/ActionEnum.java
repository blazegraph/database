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
 * Created on Jun 27, 2008
 */

package com.bigdata.relation.rule.eval;

import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.rule.IProgram;

/**
 * Symbolic constants corresponding to the type of action associated with the
 * execution of an {@link IProgram}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public enum ActionEnum {

    /**
     * The computed {@link ISolution}s will be materialized for the client.
     */
    Query(false/*mutation*/),

    /**
     * Write the computed {@link ISolution}s on the {@link IMutableRelation}.
     */
    Insert(true/*mutation*/),

    /**
     * Delete the computed {@link ISolution}s from the {@link IMutableRelation}.
     */
    Delete(true/*mutation*/);

    // /**
    // * Apply an {@link ITransform} to the elements of the
    // * {@link IMutableRelation} selected by the computed {@link ISolution}s.
    // */
    // Update;

    final private boolean mutation;
    
    private ActionEnum(final boolean mutation) {

        this.mutation = mutation;
        
    }

    /**
     * True iff the {@link ActionEnum} is one of those that writes on the
     * relation (vs query).
     */
    public boolean isMutation() {
        
        return mutation;
        
    }
    
}
