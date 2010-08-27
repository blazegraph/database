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
 * Created on Jun 18, 2006
 */
package com.bigdata.bop.engine;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.bigdata.bop.engine.BufferService.ReadBufferTask;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.util.config.NicUtil;

/**
 * Test verifies the ability to transmit a file using the
 * {@link BufferService}.
 * 
 * @version $Id$
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson
 *         </a>
 */
public class TestReceiveBuffer extends TestCase3 {

    /**
     * 
     */
    public TestReceiveBuffer() {
        super();
    }

    public TestReceiveBuffer(String name) {
        super(name);
    }

    /**
     * Test the ability to receive a {@link ByteBuffer}.
     * 
     * @throws Exception
     * 
     * @todo do test where the receive buffer is too small or is setup with a
     *       position and limit which are not sufficient to receive the data
     *       from the source buffer.
     */
    public void test_receiveBuffer() throws Exception {
        
        final UUID allowedUUID = UUID.randomUUID();

        final ByteBuffer allowedBuffer = DirectBufferPool.INSTANCE.acquire(1,
                TimeUnit.SECONDS);
        
        try {

            // populate with some random data.
            fillBufferWithRandomData(allowedBuffer);
            
            // note the current position and limit.
            final int pos = allowedBuffer.position();
            final int limit = allowedBuffer.limit();

            if (log.isInfoEnabled())
                log.info("allowedUUID=" + allowedUUID + ", allowedBuffer: "
                        + allowedBuffer);

            final BufferService service = new BufferService() {

                @Override
                protected ByteBuffer getBuffer(final UUID uuid) {

                    if (allowedUUID.equals(uuid)) {

                        // allowed.
                        return allowedBuffer;

                    }

                    log.warn("Not allowed: " + uuid);

                    // Not allowed.
                    return null;

                }

                @Override
                protected File getResource(UUID uuid) throws Exception {
                    // No such file.
                    return null;
                }

            };

            final InetAddress thisInetAddr = InetAddress.getByName(NicUtil
                    .getIpAddress("default.nic", "default", true));

            // acquire the receive buffer from the pool.
            final ByteBuffer receiveBuffer = DirectBufferPool.INSTANCE.acquire(
                    1, TimeUnit.SECONDS);

            try {

                service.awaitRunning(100, TimeUnit.MILLISECONDS);

                assertTrue(service.isOpen());

                final ByteBuffer received = new ReadBufferTask(thisInetAddr,
                        service.port, allowedUUID, receiveBuffer).call();

                /*
                 * Verify that the position and limit were not modified by the
                 * transfer.
                 */ 
                assertEquals(pos,allowedBuffer.position());
                assertEquals(limit,allowedBuffer.limit());
                
                /*
                 * Verify that the returned buffer has the same data (the
                 * position of the data in the buffer may be different).
                 */
                assertEquals(allowedBuffer, received);
                
                if (log.isInfoEnabled())
                    log.info(service.counters.getCounters());

            } finally {

                // release the buffer back to the pool.
                DirectBufferPool.INSTANCE.release(receiveBuffer);

                // shutdown the service.
                service.shutdownNow();

                // verify service is down.
                assertFalse(service.isOpen());

            }
        } finally {

            // release the buffer back to the pool.
            DirectBufferPool.INSTANCE.release(allowedBuffer);

        }

    }

    /**
     * Stress test for concurrent receive of buffers.
     * 
     * @throws IOException
     * @throws TimeoutException
     * @throws InterruptedException
     */
    public void test_receiveBuffer_stress() throws IOException,
            InterruptedException, TimeoutException {

        final Random r = new Random();
        
        final InetAddress thisInetAddr = InetAddress.getByName(NicUtil
                .getIpAddress("default.nic", "default", true));

        final ConcurrentHashMap<UUID, ByteBuffer> buffers = new ConcurrentHashMap<UUID, ByteBuffer>();
        
        final BufferService service = new BufferService() {

            @Override
            protected ByteBuffer getBuffer(UUID uuid) {

                return buffers.get(uuid);

            }

            @Override
            protected File getResource(UUID uuid) throws Exception {
                // No such resource.
                return null;
            }

        };

        final int nbuffers = 20;
        final int nthreads = 10;
        final int ntasks = 40;
        
        final ExecutorService exService = Executors.newFixedThreadPool(nbuffers,
                DaemonThreadFactory.defaultThreadFactory());

        try {
            
            ((ThreadPoolExecutor) exService).prestartAllCoreThreads();

            service.awaitRunning(100, TimeUnit.MILLISECONDS);

            assertTrue(service.isOpen());

            // setup buffers with random data.
            final UUID[] uuids = new UUID[nbuffers];
            for (int i = 0; i < nbuffers; i++) {
                final ByteBuffer b;
                buffers.put(uuids[i] = UUID.randomUUID(),
                        b = DirectBufferPool.INSTANCE.acquire());
                fillBufferWithRandomData(b);
            }

            // setup concurrent tasks.
            final List<Callable<Void>> tasks = new LinkedList<Callable<Void>>();
            
            for (int i = 0; i < ntasks; i++) {

                tasks.add(new Callable<Void>() {
                    public Void call() throws Exception {
                        final UUID uuid = uuids[r
                                                .nextInt(nbuffers)];
                        final ByteBuffer expected = buffers.get(uuid);

                        final ByteBuffer tmp = DirectBufferPool.INSTANCE
                                .acquire();
                        try {

                            tmp.clear();

                            final ByteBuffer actual = new ReadBufferTask(
                                    thisInetAddr, service.port, uuid, tmp)
                                    .call();

                            /*
                             * Verify that the returned buffer has the same data
                             * (the position of the data in the buffer may be
                             * different).
                             */
                            assertEquals(expected, actual);
                        } finally {
                            DirectBufferPool.INSTANCE.release(tmp);
                        }
                        return null;
                    }
                });

            }

            final List<Future<Void>> futures = exService.invokeAll(tasks);
            
            // verify no errors.
            int nerrs = 0;
            for(Future<?> f : futures) {
                try {
                    f.get();
                } catch (ExecutionException e) {
                    nerrs++;
                    log.error(e, e);
                }
            }
            if (nerrs > 0)
                fail("There were " + nerrs + " errors: nbuffers=" + nbuffers
                        + ", nthreads=" + nthreads + ", ntasks=" + ntasks);
            
        } finally {

            exService.shutdownNow();
            
            // shutdown the service.
            service.shutdownNow();

            // verify service is down.
            assertFalse(service.isOpen());

            if (log.isInfoEnabled())
                log.info(service.counters.getCounters());

            // release the allocated buffers.
            for (ByteBuffer b : buffers.values()) {

                DirectBufferPool.INSTANCE.release(b);
                
            }

        }

    }    
}
