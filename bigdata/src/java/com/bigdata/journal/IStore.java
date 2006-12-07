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
 * Created on Oct 25, 2006
 */

package com.bigdata.journal;

import java.nio.ByteBuffer;

/**
 * Interface for reading and writing persistent data.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo refactor to reflect direct use of the index into which the data are
 *       being inserted, e.g., an object index (int keys) or some other index
 *       with arbitrary key type.
 */
public interface IStore {

    /**
     * Read the current version of the data from the store.
     * 
     * @param id
     *            The int32 within-segment persistent identifier.
     * @param dst
     *            When non-null and having sufficient bytes remaining, the data
     *            version will be read into this buffer. If null or if the
     *            buffer does not have sufficient bytes remaining, then a new
     *            (non-direct) buffer will be allocated that is right-sized for
     *            the data version, the data will be read into that buffer, and
     *            the buffer will be returned to the caller.
     * 
     * @return The data. The position will always be zero if a new buffer was
     *         allocated. Otherwise, the position will be invariant across this
     *         method. The limit - position will be the #of bytes read into the
     *         buffer, starting at the position. A <code>null</code> return
     *         indicates that the object was not found in the journal, in which
     *         case the application MUST attempt to resolve the object against
     *         the database (i.e., the object MAY have been migrated onto the
     *         database and the version logically deleted on the journal).
     * 
     * @exception DataDeletedException
     *                if the current version of the identifier data has been
     *                deleted within the scope visible to the transaction. The
     *                caller MUST NOT read through to the database if the data
     *                were deleted.
     * 
     * @exception IllegalArgumentException
     *                if the persistent identifier is non-positive (0 is treated
     *                as a null).
     * @exception IllegalStateException
     *                if the store is not open.
     * @exception IllegalStateException
     *                if the interface is transactional and the transaction is
     *                not active.
     */
    public ByteBuffer read(int id, ByteBuffer dst);

    /**
     * Write a data version. The data version of the data will not be visible
     * outside of this transaction until the transaction is committed.
     * 
     * @param id
     *            The int32 within-segment persistent identifier.
     * @param data
     *            The data to be written. The bytes from
     *            {@link ByteBuffer#position()} to {@link ByteBuffer#limit()}
     *            will be written. The position will be advanced to the limit.
     * 
     * @exception IllegalArgumentException
     *                if the persistent identifier is non-positive (0 is treated
     *                as a null).
     * @exception DataDeletedException
     *                if the persistent identifier is deleted.
     * @exception IllegalStateException
     *                if the store is not open.
     * @exception IllegalStateException
     *                if the interface is transactional and the transaction is
     *                not active.
     */
    public void write(int id, ByteBuffer data);

    /**
     * Delete the data from the store.
     * 
     * @param id
     *            The int32 within-segment persistent identifier.
     *            
     * @exception DataDeletedException
     *                if the persistent identifier is already deleted.
     *                
     * @exception IllegalArgumentException
     *                if the persistent identifier is non-positive (0 is treated
     *                as a null).
     * @exception IllegalStateException
     *                if the store is not open.
     * @exception IllegalStateException
     *                if the interface is transactional and the transaction is
     *                not active.
     */
    public void delete(int id);

}
