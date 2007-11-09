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
 * Created on Feb 16, 2007
 */

package com.bigdata.journal;

import com.bigdata.rawstore.Addr;

/**
 * An interface providing a read-only view of a commit record. A commit record
 * is written on each commit. The basic metadata in the commit record are the
 * root addresses from which various critical resources may be loaded, e.g.,
 * data structures for mapping index names to their addresses on the
 * {@link Journal}, etc. The {@link Journal} maintains an
 * {@link Journal#getCommitRecord()} index over the commits records so that
 * {@link Tx transactions} can rapidly recover the commit record corresponding
 * to their historical, read-only ground state.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo consider modifying to allow named root addresses for a small #of root
 *       names. E.g., an set of {name,addr} pairs that is either scanned as an
 *       array list or lookup using a hash table.
 */
public interface ICommitRecord {

    /**
     * The #of root ids. Their indices are [0:N-1].
     */
    static public final int MAX_ROOT_ADDRS       = 50;

    /**
     * The first root address that may be used for a user-defined object. User
     * defined root addresses begin at index 10. The first 10 root addresses are
     * reserved for use by the bigdata architecture.
     */
    static public final int FIRST_USER_ROOT      = 10;

    /**
     * The timestamp assigned to this commit record -or- <code>0L</code> iff
     * there is no {@link ICommitRecord} written on the {@link Journal}.
     */
    public long getTimestamp();

    /**
     * The commit counter associated with the commit record. This is used by
     * transactions in order to determine whether or not intervening commits
     * have occurred since the transaction start time.
     */
    public long getCommitCounter();
    
    /**
     * The #of allowed root addresses.
     */
    public int getRootAddrCount();
    
    /**
     * The last address stored in the specified root address in this
     * commit record.
     * 
     * @param index
     *            The index of the root address.
     * 
     * @return The address stored at that index.
     * 
     * @exception IndexOutOfBoundsException
     *                if the index is negative or too large.
     */
    public long getRootAddr(int index);
    
}
