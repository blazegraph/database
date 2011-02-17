/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
package com.bigdata.jsr166;
import junit.framework.*;
import java.util.*;
import java.util.concurrent.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import java.io.*;

import com.bigdata.jsr166.JSR166TestCase.CheckedRunnable;
import com.bigdata.jsr166.JSR166TestCase.ThreadShouldThrow;

public class LinkedBlockingDequeTest extends JSR166TestCase {

    public static class Unbounded extends BlockingQueueTest {
        protected BlockingQueue emptyCollection() {
            return new LinkedBlockingDeque();
        }
    }

    public static class Bounded extends BlockingQueueTest {
        protected BlockingQueue emptyCollection() {
            return new LinkedBlockingDeque(20);
        }
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static Test suite() {
        return newTestSuite(LinkedBlockingDequeTest.class,
                            new Unbounded().testSuite(),
                            new Bounded().testSuite());
    }

    /**
     * Create a deque of given size containing consecutive
     * Integers 0 ... n.
     */
    private LinkedBlockingDeque<Integer> populatedDeque(int n) {
        LinkedBlockingDeque<Integer> q =
            new LinkedBlockingDeque<Integer>(n);
        assertTrue(q.isEmpty());
        for (int i = 0; i < n; i++)
            assertTrue(q.offer(new Integer(i)));
        assertFalse(q.isEmpty());
        assertEquals(0, q.remainingCapacity());
        assertEquals(n, q.size());
        return q;
    }

    /**
     * isEmpty is true before add, false after
     */
    public void testEmpty() {
        LinkedBlockingDeque q = new LinkedBlockingDeque();
        assertTrue(q.isEmpty());
        q.add(new Integer(1));
        assertFalse(q.isEmpty());
        q.add(new Integer(2));
        q.removeFirst();
        q.removeFirst();
        assertTrue(q.isEmpty());
    }

    /**
     * size changes when elements added and removed
     */
    public void testSize() {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            assertEquals(SIZE-i, q.size());
            q.removeFirst();
        }
        for (int i = 0; i < SIZE; ++i) {
            assertEquals(i, q.size());
            q.add(new Integer(i));
        }
    }

    /**
     * offer(null) throws NPE
     */
    public void testOfferFirstNull() {
        try {
            LinkedBlockingDeque q = new LinkedBlockingDeque();
            q.offerFirst(null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * OfferFirst succeeds
     */
    public void testOfferFirst() {
        LinkedBlockingDeque q = new LinkedBlockingDeque();
        assertTrue(q.offerFirst(new Integer(0)));
        assertTrue(q.offerFirst(new Integer(1)));
    }

    /**
     * OfferLast succeeds
     */
    public void testOfferLast() {
        LinkedBlockingDeque q = new LinkedBlockingDeque();
        assertTrue(q.offerLast(new Integer(0)));
        assertTrue(q.offerLast(new Integer(1)));
    }

    /**
     * pollFirst succeeds unless empty
     */
    public void testPollFirst() {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            assertEquals(i, q.pollFirst());
        }
        assertNull(q.pollFirst());
    }

    /**
     * pollLast succeeds unless empty
     */
    public void testPollLast() {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        for (int i = SIZE-1; i >= 0; --i) {
            assertEquals(i, q.pollLast());
        }
        assertNull(q.pollLast());
    }

    /**
     * peekFirst returns next element, or null if empty
     */
    public void testPeekFirst() {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            assertEquals(i, q.peekFirst());
            assertEquals(i, q.pollFirst());
            assertTrue(q.peekFirst() == null ||
                       !q.peekFirst().equals(i));
        }
        assertNull(q.peekFirst());
    }

    /**
     * peek returns next element, or null if empty
     */
    public void testPeek() {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            assertEquals(i, q.peek());
            assertEquals(i, q.pollFirst());
            assertTrue(q.peek() == null ||
                       !q.peek().equals(i));
        }
        assertNull(q.peek());
    }

    /**
     * peekLast returns next element, or null if empty
     */
    public void testPeekLast() {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        for (int i = SIZE-1; i >= 0; --i) {
            assertEquals(i, q.peekLast());
            assertEquals(i, q.pollLast());
            assertTrue(q.peekLast() == null ||
                       !q.peekLast().equals(i));
        }
        assertNull(q.peekLast());
    }

    /**
     * getFirst() returns first element, or throws NSEE if empty
     */
    public void testFirstElement() {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            assertEquals(i, q.getFirst());
            assertEquals(i, q.pollFirst());
        }
        try {
            q.getFirst();
            shouldThrow();
        } catch (NoSuchElementException success) {}
        assertNull(q.peekFirst());
    }

    /**
     * getLast() returns last element, or throws NSEE if empty
     */
    public void testLastElement() {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        for (int i = SIZE-1; i >= 0; --i) {
            assertEquals(i, q.getLast());
            assertEquals(i, q.pollLast());
        }
        try {
            q.getLast();
            shouldThrow();
        } catch (NoSuchElementException success) {}
        assertNull(q.peekLast());
    }

    /**
     * removeFirst() removes first element, or throws NSEE if empty
     */
    public void testRemoveFirst() {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            assertEquals(i, q.removeFirst());
        }
        try {
            q.removeFirst();
            shouldThrow();
        } catch (NoSuchElementException success) {}
        assertNull(q.peekFirst());
    }

    /**
     * removeLast() removes last element, or throws NSEE if empty
     */
    public void testRemoveLast() {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        for (int i = SIZE - 1; i >= 0; --i) {
            assertEquals(i, q.removeLast());
        }
        try {
            q.removeLast();
            shouldThrow();
        } catch (NoSuchElementException success) {}
        assertNull(q.peekLast());
    }

    /**
     * remove removes next element, or throws NSEE if empty
     */
    public void testRemove() {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            assertEquals(i, q.remove());
        }
        try {
            q.remove();
            shouldThrow();
        } catch (NoSuchElementException success) {}
    }

    /**
     * removeFirstOccurrence(x) removes x and returns true if present
     */
    public void testRemoveFirstOccurrence() {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        for (int i = 1; i < SIZE; i+=2) {
            assertTrue(q.removeFirstOccurrence(new Integer(i)));
        }
        for (int i = 0; i < SIZE; i+=2) {
            assertTrue(q.removeFirstOccurrence(new Integer(i)));
            assertFalse(q.removeFirstOccurrence(new Integer(i+1)));
        }
        assertTrue(q.isEmpty());
    }

    /**
     * removeLastOccurrence(x) removes x and returns true if present
     */
    public void testRemoveLastOccurrence() {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        for (int i = 1; i < SIZE; i+=2) {
            assertTrue(q.removeLastOccurrence(new Integer(i)));
        }
        for (int i = 0; i < SIZE; i+=2) {
            assertTrue(q.removeLastOccurrence(new Integer(i)));
            assertFalse(q.removeLastOccurrence(new Integer(i+1)));
        }
        assertTrue(q.isEmpty());
    }

    /**
     * peekFirst returns element inserted with addFirst
     */
    public void testAddFirst() {
        LinkedBlockingDeque q = populatedDeque(3);
        q.pollLast();
        q.addFirst(four);
        assertSame(four, q.peekFirst());
    }

    /**
     * peekLast returns element inserted with addLast
     */
    public void testAddLast() {
        LinkedBlockingDeque q = populatedDeque(3);
        q.pollLast();
        q.addLast(four);
        assertSame(four, q.peekLast());
    }


    /**
     * A new deque has the indicated capacity, or Integer.MAX_VALUE if
     * none given
     */
    public void testConstructor1() {
        assertEquals(SIZE, new LinkedBlockingDeque(SIZE).remainingCapacity());
        assertEquals(Integer.MAX_VALUE, new LinkedBlockingDeque().remainingCapacity());
    }

    /**
     * Constructor throws IAE if capacity argument nonpositive
     */
    public void testConstructor2() {
        try {
            LinkedBlockingDeque q = new LinkedBlockingDeque(0);
            shouldThrow();
        } catch (IllegalArgumentException success) {}
    }

    /**
     * Initializing from null Collection throws NPE
     */
    public void testConstructor3() {
        try {
            LinkedBlockingDeque q = new LinkedBlockingDeque(null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * Initializing from Collection of null elements throws NPE
     */
    public void testConstructor4() {
        try {
            Integer[] ints = new Integer[SIZE];
            LinkedBlockingDeque q = new LinkedBlockingDeque(Arrays.asList(ints));
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * Initializing from Collection with some null elements throws NPE
     */
    public void testConstructor5() {
        try {
            Integer[] ints = new Integer[SIZE];
            for (int i = 0; i < SIZE-1; ++i)
                ints[i] = new Integer(i);
            LinkedBlockingDeque q = new LinkedBlockingDeque(Arrays.asList(ints));
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * Deque contains all elements of collection used to initialize
     */
    public void testConstructor6() {
        Integer[] ints = new Integer[SIZE];
        for (int i = 0; i < SIZE; ++i)
            ints[i] = new Integer(i);
        LinkedBlockingDeque q = new LinkedBlockingDeque(Arrays.asList(ints));
        for (int i = 0; i < SIZE; ++i)
            assertEquals(ints[i], q.poll());
    }

    /**
     * Deque transitions from empty to full when elements added
     */
    public void testEmptyFull() {
        LinkedBlockingDeque q = new LinkedBlockingDeque(2);
        assertTrue(q.isEmpty());
        assertEquals("should have room for 2", 2, q.remainingCapacity());
        q.add(one);
        assertFalse(q.isEmpty());
        q.add(two);
        assertFalse(q.isEmpty());
        assertEquals(0, q.remainingCapacity());
        assertFalse(q.offer(three));
    }

    /**
     * remainingCapacity decreases on add, increases on remove
     */
    public void testRemainingCapacity() {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            assertEquals(i, q.remainingCapacity());
            assertEquals(SIZE-i, q.size());
            q.remove();
        }
        for (int i = 0; i < SIZE; ++i) {
            assertEquals(SIZE-i, q.remainingCapacity());
            assertEquals(i, q.size());
            q.add(new Integer(i));
        }
    }

    /**
     * offer(null) throws NPE
     */
    public void testOfferNull() {
        try {
            LinkedBlockingDeque q = new LinkedBlockingDeque(1);
            q.offer(null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * add(null) throws NPE
     */
    public void testAddNull() {
        try {
            LinkedBlockingDeque q = new LinkedBlockingDeque(1);
            q.add(null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * push(null) throws NPE
     */
    public void testPushNull() {
        try {
            LinkedBlockingDeque q = new LinkedBlockingDeque(1);
            q.push(null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * push succeeds if not full; throws ISE if full
     */
    public void testPush() {
        try {
            LinkedBlockingDeque q = new LinkedBlockingDeque(SIZE);
            for (int i = 0; i < SIZE; ++i) {
                Integer I = new Integer(i);
                q.push(I);
                assertEquals(I, q.peek());
            }
            assertEquals(0, q.remainingCapacity());
            q.push(new Integer(SIZE));
            shouldThrow();
        } catch (IllegalStateException success) {}
    }

    /**
     * peekFirst returns element inserted with push
     */
    public void testPushWithPeek() {
        LinkedBlockingDeque q = populatedDeque(3);
        q.pollLast();
        q.push(four);
        assertSame(four, q.peekFirst());
    }


    /**
     * pop removes next element, or throws NSEE if empty
     */
    public void testPop() {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            assertEquals(i, q.pop());
        }
        try {
            q.pop();
            shouldThrow();
        } catch (NoSuchElementException success) {}
    }


    /**
     * Offer succeeds if not full; fails if full
     */
    public void testOffer() {
        LinkedBlockingDeque q = new LinkedBlockingDeque(1);
        assertTrue(q.offer(zero));
        assertFalse(q.offer(one));
    }

    /**
     * add succeeds if not full; throws ISE if full
     */
    public void testAdd() {
        try {
            LinkedBlockingDeque q = new LinkedBlockingDeque(SIZE);
            for (int i = 0; i < SIZE; ++i) {
                assertTrue(q.add(new Integer(i)));
            }
            assertEquals(0, q.remainingCapacity());
            q.add(new Integer(SIZE));
            shouldThrow();
        } catch (IllegalStateException success) {}
    }

    /**
     * addAll(null) throws NPE
     */
    public void testAddAll1() {
        try {
            LinkedBlockingDeque q = new LinkedBlockingDeque(1);
            q.addAll(null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * addAll(this) throws IAE
     */
    public void testAddAllSelf() {
        try {
            LinkedBlockingDeque q = populatedDeque(SIZE);
            q.addAll(q);
            shouldThrow();
        } catch (IllegalArgumentException success) {}
    }

    /**
     * addAll of a collection with null elements throws NPE
     */
    public void testAddAll2() {
        try {
            LinkedBlockingDeque q = new LinkedBlockingDeque(SIZE);
            Integer[] ints = new Integer[SIZE];
            q.addAll(Arrays.asList(ints));
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * addAll of a collection with any null elements throws NPE after
     * possibly adding some elements
     */
    public void testAddAll3() {
        try {
            LinkedBlockingDeque q = new LinkedBlockingDeque(SIZE);
            Integer[] ints = new Integer[SIZE];
            for (int i = 0; i < SIZE-1; ++i)
                ints[i] = new Integer(i);
            q.addAll(Arrays.asList(ints));
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * addAll throws ISE if not enough room
     */
    public void testAddAll4() {
        try {
            LinkedBlockingDeque q = new LinkedBlockingDeque(1);
            Integer[] ints = new Integer[SIZE];
            for (int i = 0; i < SIZE; ++i)
                ints[i] = new Integer(i);
            q.addAll(Arrays.asList(ints));
            shouldThrow();
        } catch (IllegalStateException success) {}
    }

    /**
     * Deque contains all elements, in traversal order, of successful addAll
     */
    public void testAddAll5() {
        Integer[] empty = new Integer[0];
        Integer[] ints = new Integer[SIZE];
        for (int i = 0; i < SIZE; ++i)
            ints[i] = new Integer(i);
        LinkedBlockingDeque q = new LinkedBlockingDeque(SIZE);
        assertFalse(q.addAll(Arrays.asList(empty)));
        assertTrue(q.addAll(Arrays.asList(ints)));
        for (int i = 0; i < SIZE; ++i)
            assertEquals(ints[i], q.poll());
    }


    /**
     * put(null) throws NPE
     */
    public void testPutNull() throws InterruptedException {
        try {
            LinkedBlockingDeque q = new LinkedBlockingDeque(SIZE);
            q.put(null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * all elements successfully put are contained
     */
    public void testPut() throws InterruptedException {
        LinkedBlockingDeque q = new LinkedBlockingDeque(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            Integer I = new Integer(i);
            q.put(I);
            assertTrue(q.contains(I));
        }
        assertEquals(0, q.remainingCapacity());
    }

    /**
     * put blocks interruptibly if full
     */
    public void testBlockingPut() throws InterruptedException {
        final LinkedBlockingDeque q = new LinkedBlockingDeque(SIZE);
        Thread t = new Thread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                for (int i = 0; i < SIZE; ++i)
                    q.put(i);
                assertEquals(SIZE, q.size());
                assertEquals(0, q.remainingCapacity());
                try {
                    q.put(99);
                    shouldThrow();
                } catch (InterruptedException success) {}
            }});

        t.start();
        Thread.sleep(SHORT_DELAY_MS);
        t.interrupt();
        t.join();
        assertEquals(SIZE, q.size());
        assertEquals(0, q.remainingCapacity());
    }

    /**
     * put blocks waiting for take when full
     */
    public void testPutWithTake() throws InterruptedException {
        final int capacity = 2;
        final LinkedBlockingDeque q = new LinkedBlockingDeque(capacity);
        Thread t = new Thread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                for (int i = 0; i < capacity + 1; i++)
                    q.put(i);
                try {
                    q.put(99);
                    shouldThrow();
                } catch (InterruptedException success) {}
            }});

        t.start();
        Thread.sleep(SHORT_DELAY_MS);
        assertEquals(q.remainingCapacity(), 0);
        assertEquals(0, q.take());
        Thread.sleep(SHORT_DELAY_MS);
        t.interrupt();
        t.join();
        assertEquals(q.remainingCapacity(), 0);
    }

    /**
     * timed offer times out if full and elements not taken
     */
    public void testTimedOffer() throws InterruptedException {
        final LinkedBlockingDeque q = new LinkedBlockingDeque(2);
        Thread t = new Thread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                q.put(new Object());
                q.put(new Object());
                assertFalse(q.offer(new Object(), SHORT_DELAY_MS, MILLISECONDS));
                try {
                    q.offer(new Object(), LONG_DELAY_MS, MILLISECONDS);
                    shouldThrow();
                } catch (InterruptedException success) {}
            }});

        t.start();
        Thread.sleep(SMALL_DELAY_MS);
        t.interrupt();
        t.join();
    }

    /**
     * take retrieves elements in FIFO order
     */
    public void testTake() throws InterruptedException {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            assertEquals(i, q.take());
        }
    }

    /**
     * Take removes existing elements until empty, then blocks interruptibly
     */
    public void testBlockingTake() throws InterruptedException {
        final LinkedBlockingDeque q = populatedDeque(SIZE);
        Thread t = new Thread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                for (int i = 0; i < SIZE; ++i) {
                    assertEquals(i, q.take());
                }
                try {
                    q.take();
                    shouldThrow();
                } catch (InterruptedException success) {}
            }});

        t.start();
        Thread.sleep(SHORT_DELAY_MS);
        t.interrupt();
        t.join();
    }


    /**
     * poll succeeds unless empty
     */
    public void testPoll() {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            assertEquals(i, q.poll());
        }
        assertNull(q.poll());
    }

    /**
     * timed poll with zero timeout succeeds when non-empty, else times out
     */
    public void testTimedPoll0() throws InterruptedException {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            assertEquals(i, q.poll(0, MILLISECONDS));
        }
        assertNull(q.poll(0, MILLISECONDS));
    }

    /**
     * timed poll with nonzero timeout succeeds when non-empty, else times out
     */
    public void testTimedPoll() throws InterruptedException {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            assertEquals(i, q.poll(SHORT_DELAY_MS, MILLISECONDS));
        }
        assertNull(q.poll(SHORT_DELAY_MS, MILLISECONDS));
    }

    /**
     * Interrupted timed poll throws InterruptedException instead of
     * returning timeout status
     */
    public void testInterruptedTimedPoll() throws InterruptedException {
        final BlockingQueue<Integer> q = populatedDeque(SIZE);
        final CountDownLatch aboutToWait = new CountDownLatch(1);
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                for (int i = 0; i < SIZE; ++i) {
                    long t0 = System.nanoTime();
                    assertEquals(i, (int) q.poll(LONG_DELAY_MS, MILLISECONDS));
                    assertTrue(millisElapsedSince(t0) < SMALL_DELAY_MS);
                }
                long t0 = System.nanoTime();
                aboutToWait.countDown();
                try {
                    q.poll(MEDIUM_DELAY_MS, MILLISECONDS);
                    shouldThrow();
                } catch (InterruptedException success) {
                    assertTrue(millisElapsedSince(t0) < MEDIUM_DELAY_MS);
                }
            }});

        aboutToWait.await();
        waitForThreadToEnterWaitState(t, SMALL_DELAY_MS);
        t.interrupt();
        awaitTermination(t, MEDIUM_DELAY_MS);
        checkEmpty(q);
    }

    /**
     * putFirst(null) throws NPE
     */
     public void testPutFirstNull() throws InterruptedException {
        try {
            LinkedBlockingDeque q = new LinkedBlockingDeque(SIZE);
            q.putFirst(null);
            shouldThrow();
        } catch (NullPointerException success) {}
     }

    /**
     * all elements successfully putFirst are contained
     */
     public void testPutFirst() throws InterruptedException {
         LinkedBlockingDeque q = new LinkedBlockingDeque(SIZE);
         for (int i = 0; i < SIZE; ++i) {
             Integer I = new Integer(i);
             q.putFirst(I);
             assertTrue(q.contains(I));
         }
         assertEquals(0, q.remainingCapacity());
    }

    /**
     * putFirst blocks interruptibly if full
     */
    public void testBlockingPutFirst() throws InterruptedException {
        final LinkedBlockingDeque q = new LinkedBlockingDeque(SIZE);
        Thread t = new Thread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                for (int i = 0; i < SIZE; ++i)
                    q.putFirst(i);
                assertEquals(SIZE, q.size());
                assertEquals(0, q.remainingCapacity());
                try {
                    q.putFirst(99);
                    shouldThrow();
                } catch (InterruptedException success) {}
            }});

        t.start();
        Thread.sleep(SHORT_DELAY_MS);
        t.interrupt();
        t.join();
        assertEquals(SIZE, q.size());
        assertEquals(0, q.remainingCapacity());
    }

    /**
     * putFirst blocks waiting for take when full
     */
    public void testPutFirstWithTake() throws InterruptedException {
        final int capacity = 2;
        final LinkedBlockingDeque q = new LinkedBlockingDeque(capacity);
        Thread t = new Thread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                for (int i = 0; i < capacity + 1; i++)
                    q.putFirst(i);
                try {
                    q.putFirst(99);
                    shouldThrow();
                } catch (InterruptedException success) {}
            }});

        t.start();
        Thread.sleep(SHORT_DELAY_MS);
        assertEquals(q.remainingCapacity(), 0);
        assertEquals(capacity - 1, q.take());
        Thread.sleep(SHORT_DELAY_MS);
        t.interrupt();
        t.join();
        assertEquals(q.remainingCapacity(), 0);
    }

    /**
     * timed offerFirst times out if full and elements not taken
     */
    public void testTimedOfferFirst() throws InterruptedException {
        final LinkedBlockingDeque q = new LinkedBlockingDeque(2);
        Thread t = new Thread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                q.putFirst(new Object());
                q.putFirst(new Object());
                assertFalse(q.offerFirst(new Object(), SHORT_DELAY_MS, MILLISECONDS));
                try {
                    q.offerFirst(new Object(), LONG_DELAY_MS, MILLISECONDS);
                    shouldThrow();
                } catch (InterruptedException success) {}
            }});

        t.start();
        Thread.sleep(SMALL_DELAY_MS);
        t.interrupt();
        t.join();
    }

    /**
     * take retrieves elements in FIFO order
     */
    public void testTakeFirst() throws InterruptedException {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            assertEquals(i, q.takeFirst());
        }
    }

    /**
     * takeFirst blocks interruptibly when empty
     */
    public void testTakeFirstFromEmpty() throws InterruptedException {
        final LinkedBlockingDeque q = new LinkedBlockingDeque(2);
        Thread t = new ThreadShouldThrow(InterruptedException.class) {
            public void realRun() throws InterruptedException {
                q.takeFirst();
            }};

        t.start();
        Thread.sleep(SHORT_DELAY_MS);
        t.interrupt();
        t.join();
    }

    /**
     * TakeFirst removes existing elements until empty, then blocks interruptibly
     */
    public void testBlockingTakeFirst() throws InterruptedException {
        final LinkedBlockingDeque q = populatedDeque(SIZE);
        Thread t = new Thread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                for (int i = 0; i < SIZE; ++i)
                    assertEquals(i, q.takeFirst());
                try {
                    q.takeFirst();
                    shouldThrow();
                } catch (InterruptedException success) {}
            }});

        t.start();
        Thread.sleep(SHORT_DELAY_MS);
        t.interrupt();
        t.join();
    }


    /**
     * timed pollFirst with zero timeout succeeds when non-empty, else times out
     */
    public void testTimedPollFirst0() throws InterruptedException {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            assertEquals(i, q.pollFirst(0, MILLISECONDS));
        }
        assertNull(q.pollFirst(0, MILLISECONDS));
    }

    /**
     * timed pollFirst with nonzero timeout succeeds when non-empty, else times out
     */
    public void testTimedPollFirst() throws InterruptedException {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            assertEquals(i, q.pollFirst(SHORT_DELAY_MS, MILLISECONDS));
        }
        assertNull(q.pollFirst(SHORT_DELAY_MS, MILLISECONDS));
    }

    /**
     * Interrupted timed pollFirst throws InterruptedException instead of
     * returning timeout status
     */
    public void testInterruptedTimedPollFirst() throws InterruptedException {
        Thread t = new Thread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                LinkedBlockingDeque q = populatedDeque(SIZE);
                for (int i = 0; i < SIZE; ++i) {
                    assertEquals(i, q.pollFirst(SHORT_DELAY_MS, MILLISECONDS));
                }
                try {
                    q.pollFirst(SMALL_DELAY_MS, MILLISECONDS);
                    shouldThrow();
                } catch (InterruptedException success) {}
            }});

        t.start();
        Thread.sleep(SHORT_DELAY_MS);
        t.interrupt();
        t.join();
    }

    /**
     * timed pollFirst before a delayed offerFirst fails; after offerFirst succeeds;
     * on interruption throws
     */
    public void testTimedPollFirstWithOfferFirst() throws InterruptedException {
        final LinkedBlockingDeque q = new LinkedBlockingDeque(2);
        Thread t = new Thread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                assertNull(q.pollFirst(SHORT_DELAY_MS, MILLISECONDS));
                assertSame(zero, q.pollFirst(LONG_DELAY_MS, MILLISECONDS));
                try {
                    q.pollFirst(LONG_DELAY_MS, MILLISECONDS);
                    shouldThrow();
                } catch (InterruptedException success) {}
            }});

        t.start();
        Thread.sleep(SMALL_DELAY_MS);
        assertTrue(q.offerFirst(zero, SHORT_DELAY_MS, MILLISECONDS));
        t.interrupt();
        t.join();
    }

    /**
     * putLast(null) throws NPE
     */
     public void testPutLastNull() throws InterruptedException {
        try {
            LinkedBlockingDeque q = new LinkedBlockingDeque(SIZE);
            q.putLast(null);
            shouldThrow();
        } catch (NullPointerException success) {}
     }

    /**
     * all elements successfully putLast are contained
     */
     public void testPutLast() throws InterruptedException {
         LinkedBlockingDeque q = new LinkedBlockingDeque(SIZE);
         for (int i = 0; i < SIZE; ++i) {
             Integer I = new Integer(i);
             q.putLast(I);
             assertTrue(q.contains(I));
         }
         assertEquals(0, q.remainingCapacity());
    }

    /**
     * putLast blocks interruptibly if full
     */
    public void testBlockingPutLast() throws InterruptedException {
        final LinkedBlockingDeque q = new LinkedBlockingDeque(SIZE);
        Thread t = new Thread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                for (int i = 0; i < SIZE; ++i)
                    q.putLast(i);
                assertEquals(SIZE, q.size());
                assertEquals(0, q.remainingCapacity());
                try {
                    q.putLast(99);
                    shouldThrow();
                } catch (InterruptedException success) {}
            }});

        t.start();
        Thread.sleep(SHORT_DELAY_MS);
        t.interrupt();
        t.join();
        assertEquals(SIZE, q.size());
        assertEquals(0, q.remainingCapacity());
    }

    /**
     * putLast blocks waiting for take when full
     */
    public void testPutLastWithTake() throws InterruptedException {
        final int capacity = 2;
        final LinkedBlockingDeque q = new LinkedBlockingDeque(capacity);
        Thread t = new Thread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                for (int i = 0; i < capacity + 1; i++)
                    q.putLast(i);
                try {
                    q.putLast(99);
                    shouldThrow();
                } catch (InterruptedException success) {}
            }});

        t.start();
        Thread.sleep(SHORT_DELAY_MS);
        assertEquals(q.remainingCapacity(), 0);
        assertEquals(0, q.take());
        Thread.sleep(SHORT_DELAY_MS);
        t.interrupt();
        t.join();
        assertEquals(q.remainingCapacity(), 0);
    }

    /**
     * timed offerLast times out if full and elements not taken
     */
    public void testTimedOfferLast() throws InterruptedException {
        final LinkedBlockingDeque q = new LinkedBlockingDeque(2);
        Thread t = new Thread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                q.putLast(new Object());
                q.putLast(new Object());
                assertFalse(q.offerLast(new Object(), SHORT_DELAY_MS, MILLISECONDS));
                try {
                    q.offerLast(new Object(), LONG_DELAY_MS, MILLISECONDS);
                    shouldThrow();
                } catch (InterruptedException success) {}
            }});

        t.start();
        Thread.sleep(SMALL_DELAY_MS);
        t.interrupt();
        t.join();
    }

    /**
     * takeLast retrieves elements in FIFO order
     */
    public void testTakeLast() throws InterruptedException {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            assertEquals(SIZE-i-1, q.takeLast());
        }
    }

    /**
     * takeLast blocks interruptibly when empty
     */
    public void testTakeLastFromEmpty() throws InterruptedException {
        final LinkedBlockingDeque q = new LinkedBlockingDeque(2);
        Thread t = new ThreadShouldThrow(InterruptedException.class) {
            public void realRun() throws InterruptedException {
                q.takeLast();
            }};

        t.start();
        Thread.sleep(SHORT_DELAY_MS);
        t.interrupt();
        t.join();
    }

    /**
     * TakeLast removes existing elements until empty, then blocks interruptibly
     */
    public void testBlockingTakeLast() throws InterruptedException {
        final LinkedBlockingDeque q = populatedDeque(SIZE);
        Thread t = new Thread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                for (int i = 0; i < SIZE; ++i)
                    assertEquals(SIZE - 1 - i, q.takeLast());
                try {
                    q.takeLast();
                    shouldThrow();
                } catch (InterruptedException success) {}
            }});

        t.start();
        Thread.sleep(SHORT_DELAY_MS);
        t.interrupt();
        t.join();
    }

    /**
     * timed pollLast with zero timeout succeeds when non-empty, else times out
     */
    public void testTimedPollLast0() throws InterruptedException {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            assertEquals(SIZE-i-1, q.pollLast(0, MILLISECONDS));
        }
        assertNull(q.pollLast(0, MILLISECONDS));
    }

    /**
     * timed pollLast with nonzero timeout succeeds when non-empty, else times out
     */
    public void testTimedPollLast() throws InterruptedException {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            assertEquals(SIZE-i-1, q.pollLast(SHORT_DELAY_MS, MILLISECONDS));
        }
        assertNull(q.pollLast(SHORT_DELAY_MS, MILLISECONDS));
    }

    /**
     * Interrupted timed pollLast throws InterruptedException instead of
     * returning timeout status
     */
    public void testInterruptedTimedPollLast() throws InterruptedException {
        Thread t = new Thread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                LinkedBlockingDeque q = populatedDeque(SIZE);
                for (int i = 0; i < SIZE; ++i) {
                    assertEquals(SIZE-i-1, q.pollLast(SHORT_DELAY_MS, MILLISECONDS));
                }
                try {
                    q.pollLast(SMALL_DELAY_MS, MILLISECONDS);
                    shouldThrow();
                } catch (InterruptedException success) {}
            }});

        t.start();
        Thread.sleep(SHORT_DELAY_MS);
        t.interrupt();
        t.join();
    }

    /**
     * timed poll before a delayed offerLast fails; after offerLast succeeds;
     * on interruption throws
     */
    public void testTimedPollWithOfferLast() throws InterruptedException {
        final LinkedBlockingDeque q = new LinkedBlockingDeque(2);
        Thread t = new Thread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                assertNull(q.poll(SHORT_DELAY_MS, MILLISECONDS));
                assertSame(zero, q.poll(LONG_DELAY_MS, MILLISECONDS));
                try {
                    q.poll(LONG_DELAY_MS, MILLISECONDS);
                    shouldThrow();
                } catch (InterruptedException success) {}
            }});

        t.start();
        Thread.sleep(SMALL_DELAY_MS);
        assertTrue(q.offerLast(zero, SHORT_DELAY_MS, MILLISECONDS));
        t.interrupt();
        t.join();
    }


    /**
     * element returns next element, or throws NSEE if empty
     */
    public void testElement() {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            assertEquals(i, q.element());
            q.poll();
        }
        try {
            q.element();
            shouldThrow();
        } catch (NoSuchElementException success) {}
    }

    /**
     * remove(x) removes x and returns true if present
     */
    public void testRemoveElement() {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        for (int i = 1; i < SIZE; i+=2) {
            assertTrue(q.contains(i));
            assertTrue(q.remove(i));
            assertFalse(q.contains(i));
            assertTrue(q.contains(i-1));
        }
        for (int i = 0; i < SIZE; i+=2) {
            assertTrue(q.contains(i));
            assertTrue(q.remove(i));
            assertFalse(q.contains(i));
            assertFalse(q.remove(i+1));
            assertFalse(q.contains(i+1));
        }
        assertTrue(q.isEmpty());
    }

    /**
     * contains(x) reports true when elements added but not yet removed
     */
    public void testContains() {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            assertTrue(q.contains(new Integer(i)));
            q.poll();
            assertFalse(q.contains(new Integer(i)));
        }
    }

    /**
     * clear removes all elements
     */
    public void testClear() {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        q.clear();
        assertTrue(q.isEmpty());
        assertEquals(0, q.size());
        assertEquals(SIZE, q.remainingCapacity());
        q.add(one);
        assertFalse(q.isEmpty());
        assertTrue(q.contains(one));
        q.clear();
        assertTrue(q.isEmpty());
    }

    /**
     * containsAll(c) is true when c contains a subset of elements
     */
    public void testContainsAll() {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        LinkedBlockingDeque p = new LinkedBlockingDeque(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            assertTrue(q.containsAll(p));
            assertFalse(p.containsAll(q));
            p.add(new Integer(i));
        }
        assertTrue(p.containsAll(q));
    }

    /**
     * retainAll(c) retains only those elements of c and reports true if changed
     */
    public void testRetainAll() {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        LinkedBlockingDeque p = populatedDeque(SIZE);
        for (int i = 0; i < SIZE; ++i) {
            boolean changed = q.retainAll(p);
            if (i == 0)
                assertFalse(changed);
            else
                assertTrue(changed);

            assertTrue(q.containsAll(p));
            assertEquals(SIZE-i, q.size());
            p.remove();
        }
    }

    /**
     * removeAll(c) removes only those elements of c and reports true if changed
     */
    public void testRemoveAll() {
        for (int i = 1; i < SIZE; ++i) {
            LinkedBlockingDeque q = populatedDeque(SIZE);
            LinkedBlockingDeque p = populatedDeque(i);
            assertTrue(q.removeAll(p));
            assertEquals(SIZE-i, q.size());
            for (int j = 0; j < i; ++j) {
                Integer I = (Integer)(p.remove());
                assertFalse(q.contains(I));
            }
        }
    }

    /**
     * toArray contains all elements in FIFO order
     */
    public void testToArray() throws InterruptedException{
        LinkedBlockingDeque q = populatedDeque(SIZE);
        Object[] o = q.toArray();
        for (int i = 0; i < o.length; i++)
            assertSame(o[i], q.poll());
    }

    /**
     * toArray(a) contains all elements in FIFO order
     */
    public void testToArray2() {
        LinkedBlockingDeque<Integer> q = populatedDeque(SIZE);
        Integer[] ints = new Integer[SIZE];
        Integer[] array = q.toArray(ints);
        assertSame(ints, array);
        for (int i = 0; i < ints.length; i++)
            assertSame(ints[i], q.remove());
    }

    /**
     * toArray(null) throws NullPointerException
     */
    public void testToArray_NullArg() {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        try {
            q.toArray(null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * toArray(incompatible array type) throws ArrayStoreException
     */
    public void testToArray1_BadArg() {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        try {
            q.toArray(new String[10]);
            shouldThrow();
        } catch (ArrayStoreException success) {}
    }


    /**
     * iterator iterates through all elements
     */
    public void testIterator() throws InterruptedException {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        Iterator it = q.iterator();
        while (it.hasNext()) {
            assertEquals(it.next(), q.take());
        }
    }

    /**
     * iterator.remove removes current element
     */
    public void testIteratorRemove() {
        final LinkedBlockingDeque q = new LinkedBlockingDeque(3);
        q.add(two);
        q.add(one);
        q.add(three);

        Iterator it = q.iterator();
        it.next();
        it.remove();

        it = q.iterator();
        assertSame(it.next(), one);
        assertSame(it.next(), three);
        assertFalse(it.hasNext());
    }


    /**
     * iterator ordering is FIFO
     */
    public void testIteratorOrdering() {
        final LinkedBlockingDeque q = new LinkedBlockingDeque(3);
        q.add(one);
        q.add(two);
        q.add(three);
        assertEquals(0, q.remainingCapacity());
        int k = 0;
        for (Iterator it = q.iterator(); it.hasNext();) {
            assertEquals(++k, it.next());
        }
        assertEquals(3, k);
    }

    /**
     * Modifications do not cause iterators to fail
     */
    public void testWeaklyConsistentIteration() {
        final LinkedBlockingDeque q = new LinkedBlockingDeque(3);
        q.add(one);
        q.add(two);
        q.add(three);
        for (Iterator it = q.iterator(); it.hasNext();) {
            q.remove();
            it.next();
        }
        assertEquals(0, q.size());
    }


    /**
     * Descending iterator iterates through all elements
     */
    public void testDescendingIterator() {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        int i = 0;
        Iterator it = q.descendingIterator();
        while (it.hasNext()) {
            assertTrue(q.contains(it.next()));
            ++i;
        }
        assertEquals(i, SIZE);
        assertFalse(it.hasNext());
        try {
            it.next();
            shouldThrow();
        } catch (NoSuchElementException success) {}
    }

    /**
     * Descending iterator ordering is reverse FIFO
     */
    public void testDescendingIteratorOrdering() {
        final LinkedBlockingDeque q = new LinkedBlockingDeque();
        for (int iters = 0; iters < 100; ++iters) {
            q.add(new Integer(3));
            q.add(new Integer(2));
            q.add(new Integer(1));
            int k = 0;
            for (Iterator it = q.descendingIterator(); it.hasNext();) {
                assertEquals(++k, it.next());
            }

            assertEquals(3, k);
            q.remove();
            q.remove();
            q.remove();
        }
    }

    /**
     * descendingIterator.remove removes current element
     */
    public void testDescendingIteratorRemove() {
        final LinkedBlockingDeque q = new LinkedBlockingDeque();
        for (int iters = 0; iters < 100; ++iters) {
            q.add(new Integer(3));
            q.add(new Integer(2));
            q.add(new Integer(1));
            Iterator it = q.descendingIterator();
            assertEquals(it.next(), new Integer(1));
            it.remove();
            assertEquals(it.next(), new Integer(2));
            it = q.descendingIterator();
            assertEquals(it.next(), new Integer(2));
            assertEquals(it.next(), new Integer(3));
            it.remove();
            assertFalse(it.hasNext());
            q.remove();
        }
    }


    /**
     * toString contains toStrings of elements
     */
    public void testToString() {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        String s = q.toString();
        for (int i = 0; i < SIZE; ++i) {
            assertTrue(s.indexOf(String.valueOf(i)) >= 0);
        }
    }


    /**
     * offer transfers elements across Executor tasks
     */
    public void testOfferInExecutor() {
        final LinkedBlockingDeque q = new LinkedBlockingDeque(2);
        q.add(one);
        q.add(two);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.execute(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                assertFalse(q.offer(three));
                assertTrue(q.offer(three, MEDIUM_DELAY_MS, MILLISECONDS));
                assertEquals(0, q.remainingCapacity());
            }});

        executor.execute(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                Thread.sleep(SMALL_DELAY_MS);
                assertSame(one, q.take());
            }});

        joinPool(executor);
    }

    /**
     * poll retrieves elements across Executor threads
     */
    public void testPollInExecutor() {
        final LinkedBlockingDeque q = new LinkedBlockingDeque(2);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.execute(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                assertNull(q.poll());
                assertSame(one, q.poll(MEDIUM_DELAY_MS, MILLISECONDS));
                assertTrue(q.isEmpty());
            }});

        executor.execute(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                Thread.sleep(SMALL_DELAY_MS);
                q.put(one);
            }});

        joinPool(executor);
    }

    /**
     * A deserialized serialized deque has same elements in same order
     */
    public void testSerialization() throws Exception {
        LinkedBlockingDeque q = populatedDeque(SIZE);

        ByteArrayOutputStream bout = new ByteArrayOutputStream(10000);
        ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(bout));
        out.writeObject(q);
        out.close();

        ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
        ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(bin));
        LinkedBlockingDeque r = (LinkedBlockingDeque)in.readObject();
        assertEquals(q.size(), r.size());
        while (!q.isEmpty())
            assertEquals(q.remove(), r.remove());
    }

    /**
     * drainTo(null) throws NPE
     */
    public void testDrainToNull() {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        try {
            q.drainTo(null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * drainTo(this) throws IAE
     */
    public void testDrainToSelf() {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        try {
            q.drainTo(q);
            shouldThrow();
        } catch (IllegalArgumentException success) {}
    }

    /**
     * drainTo(c) empties deque into another collection c
     */
    public void testDrainTo() {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        ArrayList l = new ArrayList();
        q.drainTo(l);
        assertEquals(q.size(), 0);
        assertEquals(l.size(), SIZE);
        for (int i = 0; i < SIZE; ++i)
            assertEquals(l.get(i), new Integer(i));
        q.add(zero);
        q.add(one);
        assertFalse(q.isEmpty());
        assertTrue(q.contains(zero));
        assertTrue(q.contains(one));
        l.clear();
        q.drainTo(l);
        assertEquals(q.size(), 0);
        assertEquals(l.size(), 2);
        for (int i = 0; i < 2; ++i)
            assertEquals(l.get(i), new Integer(i));
    }

    /**
     * drainTo empties full deque, unblocking a waiting put.
     */
    public void testDrainToWithActivePut() throws InterruptedException {
        final LinkedBlockingDeque q = populatedDeque(SIZE);
        Thread t = new Thread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                q.put(new Integer(SIZE+1));
            }});

        t.start();
        ArrayList l = new ArrayList();
        q.drainTo(l);
        assertTrue(l.size() >= SIZE);
        for (int i = 0; i < SIZE; ++i)
            assertEquals(l.get(i), new Integer(i));
        t.join();
        assertTrue(q.size() + l.size() >= SIZE);
    }

    /**
     * drainTo(null, n) throws NPE
     */
    public void testDrainToNullN() {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        try {
            q.drainTo(null, 0);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * drainTo(this, n) throws IAE
     */
    public void testDrainToSelfN() {
        LinkedBlockingDeque q = populatedDeque(SIZE);
        try {
            q.drainTo(q, 0);
            shouldThrow();
        } catch (IllegalArgumentException success) {}
    }

    /**
     * drainTo(c, n) empties first min(n, size) elements of queue into c
     */
    public void testDrainToN() {
        LinkedBlockingDeque q = new LinkedBlockingDeque();
        for (int i = 0; i < SIZE + 2; ++i) {
            for (int j = 0; j < SIZE; j++)
                assertTrue(q.offer(new Integer(j)));
            ArrayList l = new ArrayList();
            q.drainTo(l, i);
            int k = (i < SIZE) ? i : SIZE;
            assertEquals(l.size(), k);
            assertEquals(q.size(), SIZE-k);
            for (int j = 0; j < k; ++j)
                assertEquals(l.get(j), new Integer(j));
            while (q.poll() != null) ;
        }
    }

}
