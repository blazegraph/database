/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Feb 12, 2007
 */

package com.bigdata.btree;

/**
 * A common interface for batch operations. Batch operations can be very
 * efficient if the keys are presented in sorted order and should be used
 * to minimize network traffic.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see IReadOnlyOperation
 */
public interface IBatchOperation {
    
    /**
     * Return the #of tuples in the operation.
     */
    public int getTupleCount();
    
    /**
     * Return the keys.
     * 
     * @todo change to use IKeyBuffer?
     */
    public byte[][] getKeys();
    
    /**
     * Apply the operation - this method may be used both to define extensible
     * batch operations and to provide default (un-optimized) implementations of
     * the batch api that are useful for derived btree implementations with
     * modified semantics.
     * 
     * @param btree
     */
    public void apply(ISimpleBTree btree);

}
