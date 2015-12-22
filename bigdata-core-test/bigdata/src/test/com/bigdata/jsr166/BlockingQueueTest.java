/*
 * Written by Doug Lea and Martin Buchholz with assistance from members
 * of JCP JSR-166 Expert Group and released to the public domain, as
 * explained at http://creativecommons.org/licenses/publicdomain
 *
 * Other contributors include Andrew Wright, Jeffrey Hayes,
 * Pat Fisher, Mike Judd.
 */
package com.bigdata.jsr166;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Contains tests generally applicable to BlockingQueue implementations.
 */
public abstract class BlockingQueueTest extends JSR166TestCase {
    /*
     * This is the start of an attempt to refactor the tests for the
     * various related implementations of related interfaces without
     * too much duplicated code.  junit does not really support such
     * testing.  Here subclasses of TestCase not only contain tests,
     * but also configuration information that describes the
     * implementation class, most importantly how to instantiate
     * instances.
     */

    /** Like suite(), but non-static */
    public Test testSuite() {
        // TODO: filter the returned tests using the configuration
        // information provided by the subclass via protected methods.
        return new TestSuite(this.getClass());
    }

    /** Returns an empty instance of the implementation class. */
    protected abstract BlockingQueue emptyCollection();

    /**
     * timed poll before a delayed offer fails; after offer succeeds;
     * on interruption throws
     */
    public void testTimedPollWithOffer() throws InterruptedException {
        final BlockingQueue q = emptyCollection();
        final CheckedBarrier barrier = new CheckedBarrier(2);
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                assertNull(q.poll(SHORT_DELAY_MS, MILLISECONDS));

                barrier.await();
                assertSame(zero, q.poll(MEDIUM_DELAY_MS, MILLISECONDS));

                Thread.currentThread().interrupt();
                try {
                    q.poll(SHORT_DELAY_MS, MILLISECONDS);
                    shouldThrow();
                } catch (InterruptedException success) {}

                barrier.await();
                try {
                    q.poll(MEDIUM_DELAY_MS, MILLISECONDS);
                    shouldThrow();
                } catch (InterruptedException success) {}
            }});

        barrier.await();
        assertTrue(q.offer(zero, SHORT_DELAY_MS, MILLISECONDS));
        barrier.await();
        sleep(SHORT_DELAY_MS);
        t.interrupt();
        awaitTermination(t, MEDIUM_DELAY_MS);
    }

    /**
     * take() blocks interruptibly when empty
     */
    public void testTakeFromEmptyBlocksInterruptibly()
            throws InterruptedException {
        final BlockingQueue q = emptyCollection();
        final CountDownLatch threadStarted = new CountDownLatch(1);
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                long t0 = System.nanoTime();
                threadStarted.countDown();
                try {
                    q.take();
                    shouldThrow();
                } catch (InterruptedException success) {}
                assertTrue(millisElapsedSince(t0) >= SHORT_DELAY_MS);
            }});

        threadStarted.await();
        Thread.sleep(SHORT_DELAY_MS);
        assertTrue(t.isAlive());
        t.interrupt();
        awaitTermination(t, MEDIUM_DELAY_MS);
    }

    /**
     * take() throws InterruptedException immediately if interrupted
     * before waiting
     */
    public void testTakeFromEmptyAfterInterrupt()
            throws InterruptedException {
        final BlockingQueue q = emptyCollection();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                long t0 = System.nanoTime();
                Thread.currentThread().interrupt();
                try {
                    q.take();
                    shouldThrow();
                } catch (InterruptedException success) {}
                assertTrue(millisElapsedSince(t0) < SMALL_DELAY_MS);
            }});

        awaitTermination(t, MEDIUM_DELAY_MS);
    }

    /** For debugging. */
    public void XXXXtestFails() {
        fail(emptyCollection().getClass().toString());
    }

}
