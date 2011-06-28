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
 * Created on Oct 1, 2010
 */

package com.bigdata.relation.rule;

import com.bigdata.relation.accesspath.EmptyAccessPath;
import com.bigdata.relation.accesspath.IAccessPath;

/**
 * An "expander" which replaces the access path with an {@link EmptyAccessPath}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class EmptyAccessPathExpander<E> implements IAccessPathExpander<E> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public static transient final EmptyAccessPathExpander INSTANCE = new EmptyAccessPathExpander();

    public IAccessPath<E> getAccessPath(IAccessPath<E> accessPath) {

        return new EmptyAccessPath<E>(accessPath.getPredicate(), accessPath
                .getKeyOrder());
        
    }

    public boolean runFirst() {

        return false;
        
    }

    public boolean backchain() {
        
        return false;
        
    }

}
