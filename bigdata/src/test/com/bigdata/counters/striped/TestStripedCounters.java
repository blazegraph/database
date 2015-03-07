/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on Apr 30, 2010
 */

package com.bigdata.counters.striped;

import junit.framework.TestCase2;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.Instrument;

/**
 * Unit tests for {@link StripedCounters}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestStripedCounters extends TestCase2 {

    /**
     * 
     */
    public TestStripedCounters() {
    }

    /**
     * @param name
     */
    public TestStripedCounters(String name) {
        super(name);
    }

    /**
     * Sample implementation used for the unit tests.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <T>
     */
    static private class StoreCounters<T extends StoreCounters<T>> extends
            StripedCounters<T> {

        /**
         * Performance counter used by the unit tests.
         */
        public volatile long nreads;

        /**
         * {@inheritDoc}
         */
        public StoreCounters() {
            super();
        }

        /**
         * {@inheritDoc}
         */
        public StoreCounters(int batchSize) {
            super(batchSize);
        }

        /**
         * {@inheritDoc}
         */
        public StoreCounters(int nstripes, int batchSize) {
            super(nstripes, batchSize);
        }

        @Override
        public void add(final T o) {

            super.add(o);
            
            nreads += o.nreads;
            
        }

        @Override
        public T subtract(final T o) {

            // make a copy of the current counters.
            final T t = super.subtract(o);
            
            // subtract out the given counters.
            t.nreads -= o.nreads;

            return t;
            
        }
        
        @Override
        public void clear() {

            // subtract out the given counters.
            nreads = 0;

        }

        @Override
        public CounterSet getCounters() {

            final CounterSet root = super.getCounters();

            root.addCounter("nreads", new Instrument<Long>() {
                public void sample() {
                    setValue(nreads);
                }
            });

            return root;

        }

    }

    /**
     * A basic unit test verifies that a child "strip" is accessed by acquire()
     * and that the updates are only pushed to the parent counters object every
     * batchSize release()s.
     * 
     * @todo do a multi-threaded unit test.
     */
    public void test_stripedCounters() {

        final int nstripes = 3;
        final int batchSize = 5;
        final StoreCounters<?> c = new StoreCounters(nstripes, batchSize);

        // do batchSize-1 updates and verify no change in the outer instance.
        for (int i = 1; i < batchSize; i++) {
            final StoreCounters<?> t = (StoreCounters<?>) c.acquire();
            if (t == c)
                fail("returned the parent instead of the child.");
            t.nreads++;
            t.release();
            if (log.isInfoEnabled())
                log.info("pass=" + i + ", value=" + c.nreads);
            if (c.nreads != 0)
                fail("updated on pass: i=" + i + ", value=" + c.nreads);
        }

        // do one more update and verify outer instance has changed.
        {
            final StoreCounters<?> t = (StoreCounters<?>) c.acquire();
            t.nreads++;
            t.release();
            if (log.isInfoEnabled())
                log.info("pass=" + batchSize + ", value=" + c.nreads);
            if (c.nreads == 0)
                fail("Counter value was not updated.");
            assertEquals(c.nreads, batchSize);
        }

        // do batchSize-1 updates and verify no change in the outer instance.
        if (log.isInfoEnabled())
            log.info("-----------------");
        final long lastValue = c.nreads;
        for (int i = 1; i < batchSize; i++) {
            final StoreCounters<?> t = (StoreCounters<?>) c.acquire();
            if (t == c)
                fail("returned the parent instead of the child.");
            t.nreads++;
            t.release();
            if (log.isInfoEnabled())
                log.info("pass=" + i + ", value=" + c.nreads);
            if (c.nreads != lastValue)
                fail("updated on pass: i=" + i + ", value=" + c.nreads);
        }

        // do one more update and verify outer instance has changed.
        {
            final StoreCounters<?> t = (StoreCounters<?>) c.acquire();
            t.nreads++;
            t.release();
            if (log.isInfoEnabled())
                log.info("pass=" + batchSize + ", value=" + c.nreads);
            if (c.nreads == lastValue)
                fail("Counter value was not updated.");
            assertEquals(c.nreads, batchSize * 2);
        }

    }

}
