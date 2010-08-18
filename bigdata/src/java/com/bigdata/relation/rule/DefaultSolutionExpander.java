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
 * Created on Sep 3, 2008
 */

package com.bigdata.relation.rule;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.rule.eval.IJoinNexus;

/**
 * A base class for {@link ISolutionExpander} implementations. The base class
 * provides various helper methods designed to make it easier to override the
 * evaluation behavior of an {@link IPredicate} during {@link IRule} evaluation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DefaultSolutionExpander implements ISolutionExpander {

    /**
     * 
     */
    private static final long serialVersionUID = -9174057768088016404L;

    public void expand(IJoinNexus joinNexus, IBindingSet bindingSet,
            IPredicate predicate, boolean isSolution) {
        
        throw new UnsupportedOperationException();
        
    }

    /**
     * Returns the given {@link IAccessPath}.
     */
    public IAccessPath getAccessPath(IAccessPath accessPath) {

        return accessPath;
        
    }

    /**
     * Returns the approximate range count for the given {@link IAccessPath}.
     */
    public long rangeCount(IAccessPath accessPath) {

        return accessPath.rangeCount(false/*exact*/);
        
    }
    
    /**
     * Default to true for backchaining.
     */
    public boolean backchain() {
        
        return true;
        
    }

    /**
     * Default to false for run first.
     */
    public boolean runFirst() {
        
        return false;
        
    }

}
