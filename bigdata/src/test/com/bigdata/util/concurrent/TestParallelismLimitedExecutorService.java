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
 * Created on Oct 29, 2008
 */

package com.bigdata.util.concurrent;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase2;

/**
 * Test suite for {@link ParallelismLimitedExecutorService}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestParallelismLimitedExecutorService extends TestCase2 {

    /**
     * 
     */
    public TestParallelismLimitedExecutorService() {

    }

    /**
     * @param arg0
     */
    public TestParallelismLimitedExecutorService(String arg0) {

        super(arg0);

    }

    /**
     * Test the ability to create and shutdown the service.
     * 
     * @throws InterruptedException 
     */
    public void test_lifeCycle_shutdown() throws InterruptedException {

        final ExecutorService delegate = Executors.newCachedThreadPool();

        try {

            final ParallelismLimitedExecutorService service = new ParallelismLimitedExecutorService(
                    delegate, 1/* maxParallel */, 1/* queueCapacity */);

            service.shutdown();

            if (!service.awaitTermination(1L, TimeUnit.SECONDS)) {

                fail("Service is not shutdown.");

            }

            assertEquals(0,service.getTaskCount());
            assertEquals(0,service.getCompletedTaskCount());
            assertEquals(0,service.getErrorCount());
            assertEquals(0,service.getCancelCount());
            assertEquals(0,service.getSuccessCount());
            assertEquals(0,service.getActiveCount());
            assertEquals(0,service.getMaxActiveCount());
            assertEquals(1,service.getQueue().remainingCapacity());
            
        } finally {

            delegate.shutdownNow();

        }

    }

    /**
     * Test ability to create a service, submit a task, and shutdown the
     * service.
     * 
     * @throws InterruptedException
     */
    public void test_lifeCycle_submit1_shutdown() throws InterruptedException {

        final ExecutorService delegate = Executors.newCachedThreadPool();

        try {

            final ParallelismLimitedExecutorService service = new ParallelismLimitedExecutorService(
                    delegate, 1/* maxParallel */, 1/*queueCapacity*/);

            service.submit(new Runnable() {
                public void run() {
                }
            });

            service.shutdown();

            if (!service.awaitTermination(1L, TimeUnit.SECONDS)) {

                fail("Service is not shutdown.");

            }

            assertEquals(1,service.getTaskCount());
            assertEquals(1,service.getCompletedTaskCount());
            assertEquals(0,service.getErrorCount());
            assertEquals(0,service.getCancelCount());
            assertEquals(1,service.getSuccessCount());
            assertEquals(0,service.getActiveCount());
            assertEquals(1,service.getMaxActiveCount());
            assertEquals(1,service.getQueue().remainingCapacity());
            
        } finally {

            delegate.shutdownNow();

        }
    }

    public void test_lifeCycle_shutdownNow() throws InterruptedException {

        final ExecutorService delegate = Executors.newCachedThreadPool();

        try {
            final ParallelismLimitedExecutorService service = new ParallelismLimitedExecutorService(
                    delegate, 1/* maxParallel */, 1/*queueCapacity*/);

            final List<Runnable> pending = service.shutdownNow();

            assertEquals(0, pending.size());

            if (!service.awaitTermination(1L, TimeUnit.SECONDS)) {

                fail("Service is not shutdown.");

            }

            assertEquals(0,service.getTaskCount());
            assertEquals(0,service.getCompletedTaskCount());
            assertEquals(0,service.getErrorCount());
            assertEquals(0,service.getCancelCount());
            assertEquals(0,service.getSuccessCount());
            assertEquals(0,service.getActiveCount());
            assertEquals(0,service.getMaxActiveCount());
            assertEquals(1,service.getQueue().remainingCapacity());

        } finally {

            delegate.shutdownNow();

        }

    }

    public void test_lifeCycle_submitAndWait_shutdownNow()
            throws InterruptedException {

        final ExecutorService delegate = Executors.newCachedThreadPool();

        try {

            final ParallelismLimitedExecutorService service = new ParallelismLimitedExecutorService(
                    delegate, 1/* maxParallel */, 1/*queueCapacity*/);

            final AtomicBoolean started = new AtomicBoolean(false);

            service.submit(new Runnable() {
                public void run() {
                    started.set(true);
                    // run until interupted (aka until cancelled).
                    while (!Thread.interrupted()) {
                        try {
                            Thread.sleep(10/* ms */);
                        } catch (InterruptedException e) {
                            // exit.
                            break;
                        }
                    }
                }
            });

            final long begin = System.currentTimeMillis();
            while (!started.get()
                    && (System.currentTimeMillis() - begin) < 1000) {
                // twiddle thumbs.
            }

            assertTrue(started.get());

            final List<Runnable> pending = service.shutdownNow();

            assertEquals(0, pending.size());

            if (!service.awaitTermination(1L, TimeUnit.SECONDS)) {

                fail("Service is not shutdown.");

            }

            assertEquals(1,service.getTaskCount());
            assertEquals(1,service.getCompletedTaskCount());
            assertEquals(0,service.getErrorCount());
            assertEquals(1,service.getCancelCount());
            assertEquals(0,service.getSuccessCount());
            assertEquals(0,service.getActiveCount());
            assertEquals(1,service.getMaxActiveCount());
            assertEquals(1,service.getQueue().remainingCapacity());

        } finally {

            delegate.shutdownNow();

        }

    }

    public void test_lifeCycle_rejectAfterShutdown()
            throws InterruptedException {

        final ExecutorService delegate = Executors.newCachedThreadPool();

        try {

            final ParallelismLimitedExecutorService service = new ParallelismLimitedExecutorService(
                    delegate, 1/* maxParallel */, 1/*queueCapacity*/);

            service.shutdown();

            try {

                service.submit(new Runnable() {
                    public void run() {
                    }
                });

                fail("Expecting: " + RejectedExecutionException.class);

            } catch (RejectedExecutionException ex) {

                log.info("Ignoring expected exception: " + ex);

            }

            assertEquals(0,service.getTaskCount());
            assertEquals(0,service.getCompletedTaskCount());
            assertEquals(0,service.getErrorCount());
            assertEquals(0,service.getCancelCount());
            assertEquals(0,service.getSuccessCount());
            assertEquals(0,service.getActiveCount());
            assertEquals(0,service.getMaxActiveCount());
            assertEquals(1,service.getQueue().remainingCapacity());

        } finally {

            delegate.shutdownNow();

        }

    }

    /**
     * Stress test that explores: (a) whether we can reach the maximum
     * parallelism; and (b) makes sure that we do not exceed the maximum
     * parallelism.
     * 
     * @throws InterruptedException 
     */
    public void test_maxParallelism() throws InterruptedException {

        final int maxParallel = 10;

        /*
         * This test uses a fixed capacity thread pool and pre-starts sufficient
         * core threads in order to reduce the latency required to run the
         * thread pool up to the maximum parallelism.
         */
        final ThreadPoolExecutor delegate;
        {
            final int corePoolSize = maxParallel + 10;
            delegate = (ThreadPoolExecutor) Executors
                    .newFixedThreadPool(corePoolSize);
            delegate.setCorePoolSize(corePoolSize);
            delegate.prestartAllCoreThreads();
        }

        try {

            final int queueCapacity = maxParallel + 10;
            final ParallelismLimitedExecutorService service = new ParallelismLimitedExecutorService(
                    delegate, maxParallel/* maxParallel */, queueCapacity);

            final AtomicInteger concurrent = new AtomicInteger(0);
            final AtomicInteger maxConcurrent = new AtomicInteger(0);

            final int LIMIT = maxParallel * 2;
            
            for (int i = 0; i < LIMIT; i++) {

                service.submit(new Runnable() {
                    public void run() {
                        final int n = concurrent.incrementAndGet();
                        try {
                            synchronized (maxConcurrent) {
                                if (n > maxConcurrent.get()) {
                                    maxConcurrent.set(n);
                                }
                            }
                            try {
                                // simulate a workload for the task.
                                Thread.sleep(10/* ms */);
                            } catch (InterruptedException e) {
                                return;
                            }
                        } finally {
                            concurrent.decrementAndGet();
                        }
                    }
                });

            }

            service.shutdown();

            service.awaitTermination(10L, TimeUnit.SECONDS);

            // reporting by the test harness.
            System.err.println("concurrent=" + concurrent + ", maxConcurrent="
                    + maxConcurrent + ", maxParallel=" + maxParallel);
            
            // self reporting by the service.
            System.err.println(service.toString());

            /*
             * This is not a hard failure but a low value here does indicate a
             * probable failure if the system performing the test is not highly
             * loaded.
             */

            assertTrue("maxParallel=" + maxParallel + ", but maxConcurrent="
                    + maxConcurrent,
                    maxConcurrent.get() > (int) (.9 * maxParallel));

            /*
             * This is a hard failure. A failure here could be either the test
             * harness or the service.
             */
            assertTrue("maxParallel=" + maxParallel + ", but maxConcurrent="
                    + maxConcurrent, maxConcurrent.get() <= maxParallel);
            
            assertEquals(LIMIT,service.getTaskCount());
            assertEquals(LIMIT,service.getCompletedTaskCount());
            assertEquals(0,service.getErrorCount());
            assertEquals(0,service.getCancelCount());
            assertEquals(LIMIT,service.getSuccessCount());
            assertEquals(0,service.getActiveCount());
            assertEquals(maxConcurrent.get(),service.getMaxActiveCount());
            assertEquals(queueCapacity,service.getQueue().remainingCapacity());

        } finally {

            delegate.shutdownNow();

        }

    }

}
