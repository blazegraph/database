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
 * Created on Aug 19, 2010
 */

package com.bigdata.bop;

import com.bigdata.relation.rule.eval.IJoinNexus;

/**
 * An interface for exposing the data in an object view of a tuple by index
 * position. This facilitates binding values elements read from an access path
 * onto binding sets during join processing.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see IPredicate#get(Object, int)
 * @see IJoinNexus#bind(IPredicate, IConstraint, Object, IBindingSet)
 */
public interface IElement {

    /**
     * Return the value at the specified index.
     * 
     * @param index
     *            The index.
     * 
     * @return The value at the specified index.
     * 
     * @throws IllegalArgumentException
     *             if the index is less than zero or GTE the #of fields defined
     *             for the element.
     */
    public Object get(int index);

}
