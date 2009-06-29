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
 * Created on Jun 26, 2009
 */

package com.bigdata.util.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.TestCase2;

/**
 * Unit tests for {@link Latch}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestLatch extends TestCase2 {

    /**
     * 
     */
    public TestLatch() {

    }

    /**
     * @param name
     */
    public TestLatch(String name) {
        super(name);

    }

    /**
     * Basic tests of the counter.
     */
    public void test1() {

        final Latch latch = new Latch();

        assertEquals(latch.get(), 0);

        assertEquals(latch.inc(), 1);

        assertEquals(latch.inc(), 2);

        assertEquals(latch.dec(), 1);

        assertEquals(latch.dec(), 0);

        try {
            latch.dec();
            fail("Expecting: " + IllegalStateException.class);
        } catch (IllegalStateException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected error: " + ex);
        }

        assertEquals(latch.get(), 0);

    }

    /**
     * Basic tests releasing blocked threads.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     * 
     * @todo should have variants of these where the expectations are violated
     *       in order to verify correct failure mores. For example, where the
     *       timeout is to short in the Callable, where the outer thread fails
     *       to dec() or where the inner thread fails to inc().
     */
    public void test2() throws InterruptedException, ExecutionException {

        final Latch latch = new Latch();

        final Callable<?> r = new Callable<Void>() {

            public Void call() throws Exception {

                latch.inc();

                if(!latch.await(100, TimeUnit.MILLISECONDS))
                    throw new TimeoutException();

                return null;

            }

        };

        final ExecutorService service = Executors
                .newSingleThreadExecutor(DaemonThreadFactory
                        .defaultThreadFactory());

        try {

            final Future<?> future = service.submit(r);

            Thread.sleep(50);

            latch.dec();

            future.get();

        } finally {

            service.shutdownNow();

        }

    }

}
