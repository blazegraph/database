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
 * Created on Jun 13, 2010
 */

package com.bigdata.ha;

import java.io.IOException;
import java.rmi.Remote;
import java.util.UUID;
import java.util.concurrent.Future;

import com.bigdata.rawstore.IRawStore;

/**
 * A {@link Remote} interface supporting low-level reads against persistent data
 * from a quorum member. This interface is used to handle bad reads, which are
 * identified by a checksum error when reading on the local disk. To handle a
 * bad read, the quorum member reads on a different member of the same quorum.
 * 
 * @see QuorumRead

 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface HAReadGlue extends Remote {

    /**
     * Read a record an {@link IRawStore} managed by the service. Services MUST
     * NOT refer this message to another service. If the read can not be
     * satisfied, for example because the {@link IRawStore} has been released or
     * because there is a checksum error when reading on the {@link IRawStore},
     * then that exception should be thrown back to the caller.
     * 
     * @param storeId
     *            The {@link UUID} identifying the {@link IRawStore} for which
     *            the record was requested.
     * @param addr
     *            The address of the record.
     * 
     * @return The {@link Future} of an operation which evaluated to the desired
     *         record.
     * 
     * @see QuorumRead#readFromQuorum(UUID, long)
     */
    public Future<byte[]> readFromDisk(final long token, UUID storeId, long addr)
            throws IOException;

}
