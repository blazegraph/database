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
 * Created on Feb 28, 2008
 */

package com.bigdata.mdi;

/**
 * Interface provides access to the left and right separator keys for an index
 * partition.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ISeparatorKeys {

    /**
     * The separator key that defines the left edge of that index partition
     * (always defined) - this is the first key that can enter the index
     * partition. The left-most separator key for a scale-out index is always an
     * empty <code>byte[]</code> since that is the smallest key that may be
     * defined.
     */
    public byte[] getLeftSeparatorKey();
    
    /**
     * The separator key that defines the right edge of that index partition or
     * <code>null</code> iff the index partition does not have a right sibling
     * (a <code>null</code> has the semantics of having no upper bound).
     */
    public byte[] getRightSeparatorKey();
    
}
