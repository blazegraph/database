/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

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
     *            The index of the root {@link Addr address}.
     * 
     * @return The {@link Addr address} stored at that index.
     * 
     * @exception IndexOutOfBoundsException
     *                if the index is negative or too large.
     */
    public long getRootAddr(int index);
    
}
