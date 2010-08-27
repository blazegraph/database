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

package com.bigdata.bop.engine;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.bigdata.bop.BOp;
import com.bigdata.io.DirectBufferPool;

/**
 * This class manages a pool of direct {@link ByteBuffer}s which are exposed for
 * retrieval by remote services.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Add a constructor argument for the maximum #of buffers which may be
 *       allocated from the pool or leave this to the annotated {@link BOp}s or
 *       to logic managing resource utilization during query evaluation? E.g.,
 *       breaking a query if it would demand too many resources given the
 *       concurrent query demand or forcing the query to block until sufficient
 *       resources are available?
 */
public class ManagedBufferService extends BufferService {

    /**
     * The pool from which the direct {@link ByteBuffer}s are allocated.
     */
    private final DirectBufferPool pool = DirectBufferPool.INSTANCE;
    
    /**
     * The set of buffers "owned" by this service.
     */
    private final ConcurrentHashMap<UUID, ByteBuffer> buffers = new ConcurrentHashMap<UUID, ByteBuffer>();

    /**
     * @throws IOException
     */
    public ManagedBufferService() throws IOException {
    }

    /**
     * @param port
     * @throws IOException
     */
    public ManagedBufferService(int port) throws IOException {
        super(port);
    }

    /**
     * @param port
     * @param requestServicePoolSize
     * @throws IOException
     */
    public ManagedBufferService(int port, int requestServicePoolSize)
            throws IOException {
        super(port, requestServicePoolSize);
    }
    
    @Override
    synchronized public void shutdown() {
        super.shutdown();
        releaseBuffers();
    }

    @Override
    synchronized public void shutdownNow() {
        super.shutdownNow();
        releaseBuffers();
    }

    /**
     * Allocate a new buffer and return its identifier.
     * 
     * @throws InterruptedException
     */
    public UUID newBuffer() throws InterruptedException {
        final UUID uuid = UUID.randomUUID();
        final ByteBuffer b = pool.acquire();
        buffers.put(uuid, b);
        return uuid;
    }

    public void releaseBuffer(final UUID uuid) throws InterruptedException {
        final ByteBuffer b = buffers.get(uuid);
        if (b == null) {
            // No such buffer.
            throw new AssertionError();
        }
        pool.release(b);
        buffers.remove(uuid);
    }

    /**
     * Release all buffers.
     */
    private void releaseBuffers() {
        boolean interrupted = false;
        for(ByteBuffer b : buffers.values()) {
            try {
                pool.release(b);
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted)
            // propagate the interrupt.
            Thread.currentThread().interrupt();
    }
    
    @Override
    protected ByteBuffer getBuffer(UUID uuid) throws Exception {
        return buffers.get(uuid);
    }

    /**
     * TODO This class should be setup to wrap an interface so we can delegate
     * this method through to the ResourceManager on the data service.
     */
    @Override
    protected File getResource(UUID uuid) throws Exception {
        return null;
    }

}
