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
import java.util.UUID;

import com.bigdata.quorum.Quorum;
import com.bigdata.rawstore.IRawStore;

/**
 * A non-remote interface for a member service in a {@link Quorum} defining
 * methods to support reading on another member of the quorum.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface QuorumRead<S extends HAReadGlue> { // extends QuorumService<S> {

    /**
     * Used by any service joined with the quorum to read a record from another
     * service joined with the quorum in order to work around a "bad read" as
     * identified by a checksum error on the local service.
     * <p>
     * Note: This is NOT the normal path for reading on a record from a service.
     * This is used to handle bad reads (when a checksum or IO error is reported
     * by the local disk) by reading the record from another member of the
     * quorum.
     * 
     * @param storeId
     *            The {@link UUID} of the {@link IRawStore} from which the
     *            record should be read.
     * @param addr
     *            The address of a record on that store.
     * 
     * @return The record.
     * 
     * @throws IllegalStateException
     *             if the quorum is not highly available.
     * @throws RuntimeException
     *             if the quorum is highly available but the record could not be
     *             read.
     * 
     * @see HAGlue#readFromDisk(UUID, long)
     */
    byte[] readFromQuorum(UUID storeId, long addr) throws InterruptedException,
            IOException;

}
