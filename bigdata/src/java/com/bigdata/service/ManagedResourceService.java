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
 * Created on Aug 25, 2010
 */

package com.bigdata.service;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.UUID;

import com.bigdata.bop.BOp;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.DirectBufferPoolAllocator;
import com.bigdata.io.DirectBufferPoolAllocator.IAllocation;

/**
 * This class manages a pool of direct {@link ByteBuffer}s. The application can
 * create which are exposed for retrieval by remote services.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: ManagedResourceService.java 3515 2010-09-08 13:16:30Z
 *          thompsonbry $
 * 
 * @todo Harmonize the constructors with the base class (and perhaps simplify)
 *       and add a constructor argument for the maximum #of buffers which may be
 *       allocated from the pool or leave this to the annotated {@link BOp}s or
 *       to logic managing resource utilization during query evaluation? E.g.,
 *       breaking a query if it would demand too many resources given the
 *       concurrent query demand or forcing the query to block until sufficient
 *       resources are available?
 */
abstract public class ManagedResourceService extends ResourceService {

    private final DirectBufferPoolAllocator allocator;

    /**
     * The object used to make, resolve, and release allocations against a
     * {@link DirectBufferPool}.
     */
    public DirectBufferPoolAllocator getAllocator() {
       
        return allocator;
        
    }
    
    /**
     * Create and start the service.
     * 
     * @param addr
     *            The IP address and port at which the service will accept
     *            connections. The port MAY be zero to use an ephemeral port.
     * @param requestServicePoolSize
     *            The size of the thread pool that will handle requests. When
     *            ZERO (0) a cached thread pool will be used with no specific
     *            size limit.
     * 
     * @throws IOException
     */
    public ManagedResourceService(final InetSocketAddress addr,
            final int requestServicePoolSize) throws IOException {

        super(addr, requestServicePoolSize);

        this.allocator = new DirectBufferPoolAllocator(
                DirectBufferPool.INSTANCE);

    }
    
    @Override
    synchronized public void shutdown() {
        super.shutdown();
        allocator.close();
    }

    @Override
    synchronized public void shutdownNow() {
        super.shutdownNow();
        allocator.close();
    }

    /**
     * {@inheritDoc}
     * 
     * @todo An allocation can be concurrently released if a query terminates.
     *       Make sure that we interrupt any transfer in progress for the
     *       allocation when the allocation is released since there is nothing
     *       to prevent the same direct {@link ByteBuffer} from being assigned
     *       to a new allocation context and new allocations made against it
     *       concurrent with any ongoing attempt to read the data from that
     *       allocation.
     */
    @Override
    protected ByteBuffer getBuffer(final UUID uuid) throws Exception {
        final IAllocation allocation = allocator.getAllocation(uuid);
        if (allocation == null)
            return null;
        return allocation.getSlice();
    }

}
