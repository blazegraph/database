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
 * Created on Jun 18, 2006
 */
package org.CognitiveWeb.bigdata;

import java.net.InetAddress;

/**
 * <p>
 * Provides services for creating, replicating, and moving the segments of a
 * database. The segment manager does NOT track host and network load
 * information and must be explicitly guided in order to provide and maintain
 * high availability and redundency for backup. Operations are asynchronous.
 * Callers are expected to register for catalog events and take appropriate
 * actions once the necessaru catalog changes have occurred. A segment will be
 * added, replicated, or deleted as an atomic operation.
 * </p>
 * 
 * @version $Id$
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson
 *         </a>
 */

public class SegmentManager {

    /**
     * 
     */
    public SegmentManager() {
        super();
    }

    /**
     * Create a new segment on the specified host(s) and registers it with the
     * database catalog.
     * 
     * @param segmentId
     *            The segment identifier. This is an object identifier. The
     *            partition and segment components are used. The page and slot
     *            components are ignored and may be zero. It is an error if
     *            there is a pre-existing segment with the same partition and
     *            segment identifier.
     * @param addrs
     *            The host(s) on which to create the segment.
     * 
     * @exception IllegalStateException
     *                If there is a segment with that identifier in the catalog.
     */
    public void addOn(long segmentId,InetAddress[] addr) {
        throw new UnsupportedOperationException();
    }

    /**
     * Replicate the segment on the identified host(s).
     * 
     * @param segmentId
     *            The segment identifier. This is an object identifier. The page
     *            and slot components are ignored and may be zero.
     * @param toAddr
     *            The host(s) on which to replicate the segment.
     */
    public void replicateOn(long segmentId, InetAddress[] toAddr) {
        throw new UnsupportedOperationException();
    }
    
    /**
     * Delete the copy of the segment on the identifier host(s).
     * 
     * @param segmentId
     *            The segment identifier. This is an object identifier. The page
     *            and slot components are ignored and may be zero.
     * @param addr
     *            The host(s).
     * @param force
     *            When true the segment will be deleted even if it is the only
     *            remaining copy of that segment in the database. <strong>The
     *            use of this flag is <em>dangerous</em>. The database
     *            provides high availability and live backup through replication
     *            of segments. Deleting all copies of a segment will eradicate
     *            the data in that segment and may render your database
     *            applications inoperative. </strong>
     * @exception IllegalArgumentException
     *                if the segment does not exist on that host.
     * @exception IllegalStateException
     *                if there are no other copies of the segment.
     */
    public void deleteFrom(long segmentId, InetAddress[] addr,
            boolean force) {
        throw new UnsupportedOperationException();
    }
    
}
