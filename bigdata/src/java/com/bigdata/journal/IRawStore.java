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
 * Created on Nov 16, 2006
 */

package com.bigdata.journal;

import java.nio.ByteBuffer;

/**
 * An interface exposing low-level persistence store operations.  This interface
 * does NOT provide transactional isolation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IRawStore {

    /**
     * Write the data on the journal. This method is not isolated and does not
     * update the object index. It operates directly on the slots in the journal
     * and returns a {@link ISlotAllocation} that may be used to recover the
     * data.
     * 
     * @param data
     *            The data. The bytes from the current position to the limit
     *            (exclusive) will be written onto the journal. The position
     *            will be updated as a side effect. An attempt will be made to
     *            write the data onto a contiguous run of slots.
     * 
     * @return A {@link ISlotAllocation} representing the slots on which the
     *         data was written. This may be used to read the data back from the
     *         journal.
     */
    public ISlotAllocation write(ByteBuffer data);

    /**
     * Reads data from the slot allocation in sequence, assembling the result in
     * a buffer. This method is not isolated.
     * 
     * @param slots
     *            The slots whose data will be read.
     * @param dst
     *            The destination buffer (optional). When specified, the data
     *            will be appended starting at the current position. If there is
     *            not enough room in the buffer then a new buffer will be
     *            allocated and used for the read operation. In either case, the
     *            position will be advanced as a side effect and the limit will
     *            equal the final position.
     * 
     * @return The data read from those slots. A new buffer will be allocated if
     *         <i>dst</i> is <code>null</code> -or- if the data will not fit
     *         in the provided buffer.
     */
    public ByteBuffer read(ISlotAllocation slots, ByteBuffer dst);

    /**
     * Deallocates the slots (no isolation).
     * 
     * @param slots
     *            The slot allocation.
     */
    public void delete(ISlotAllocation slots);
    
}
