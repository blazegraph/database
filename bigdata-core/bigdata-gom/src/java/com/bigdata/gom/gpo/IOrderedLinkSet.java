/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Mar 22, 2012
 */

package com.bigdata.gom.gpo;

/**
 * An ordered link set.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IOrderedLinkSet extends ILinkSet {

    /**
     * {@inheritDoc}
     * <p>
     * This value is known directly to the link set and is available in O(1).
     */
    public int size();

    /**
     * {@inheritDoc}
     * <p>
     * This value is known directly to the link set and is available in O(1).
     */
    public long size2();

    /**
     * Returns the first member of the link set or <code>null</code> iff the
     * link set is empty.
     */
    public IGPO getFirst();

    /**
     * Returns the last member of the link set or <code>null</code> iff the link
     * set is empty.
     */
    public IGPO getLast();

    /**
     * Returns the next member of the link set or <code>null</code> iff
     * <i>member</i> is the last member of the link set.
     * 
     * @param member
     *            A member of the link set.
     * 
     * @exception IllegalArgumentException
     *                if <i>member</i> is <code>null</code> or otherwise not a
     *                member of the link set.
     */
    public IGPO getNext(IGPO member);

    /**
     * Returns the prior member of the link set or <code>null</code> iff
     * <i>member</i> is the first member of the link set.
     * 
     * @param member
     *            A member of the link set.
     * 
     * @exception IllegalArgumentException
     *                if <i>member</i> is <code>null</code> or otherwise not a
     *                member of the link set.
     */
    public IGPO getPrior(IGPO member);

}
