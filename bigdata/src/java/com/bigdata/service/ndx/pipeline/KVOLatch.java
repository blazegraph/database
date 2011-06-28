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
 * Created on Apr 22, 2009
 */

package com.bigdata.service.ndx.pipeline;

import com.bigdata.util.concurrent.Latch;

/**
 * A synchronization aid that allows one or more threads to await asynchronous
 * writes on one or more scale-out indices. Once the counter reaches zero, all
 * waiting threads are released. The counter is decremented automatically once a
 * {@link KVOC} has been successfully written onto an index by an asynchronous
 * write operation. This class may also be used as a synchronization aid
 * independent of a {@link KVOC}
 * <p>
 * Since it is possible that the counter could be transiently zero as chunks are
 * being added and drained concurrently, you MUST {@link #inc() increment} the
 * counter before adding the first chunk and {@link #dec() decrement} the
 * counter after adding the last chunk. Threads may invoke {@link #await()} any
 * time after the counter has been incremented. They will not be released until
 * the counter is zero. If you have protected against transient zeros by
 * pre-incrementing the counter, the threads will not be released until the
 * asynchronous write operations are successfully completed.
 * <p>
 * The notification is based on {@link KVOC}, which associates an atomic counter
 * and a user-defined key with each tuple. Notices are generated when the atomic
 * counter is zero on decrement. Notices are aligned with the appropriate scope
 * by creating an instance of the {@link KVOScope} with that scope and then
 * pairing it with each {@link KVOC}.
 * 
 * @see KVOC
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <K>
 *            The generic type of the user-defined key.
 * 
 * @see IndexPartitionWriteTask, which is responsible for invoking
 *      {@link #dec()}.
 */
public class KVOLatch extends Latch {

    public KVOLatch() {

    }

}
