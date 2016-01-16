/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Jan 2, 2012
 */

package com.bigdata.cache;

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import com.bigdata.util.Bytes;

import junit.framework.TestCase2;

/**
 * Test suite for {@link ConcurrentWeakValueCacheWithBatchedUpdates}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestConcurrentWeakValueCacheWithBatchingUpdates extends TestCase2 {

    /**
     * 
     */
    public TestConcurrentWeakValueCacheWithBatchingUpdates() {
    }

    /**
     * @param name
     */
    public TestConcurrentWeakValueCacheWithBatchingUpdates(String name) {
        super(name);
    }

    public void test_memoryLeak() {
    
        final ConcurrentWeakValueCacheWithBatchedUpdates<Long, String> fixture = new ConcurrentWeakValueCacheWithBatchedUpdates<Long, String>(//
                16,// backing hard reference LRU queue capacity.
                .75f, // loadFactor (.75 is the default)
                16 // concurrency level (16 is the default)
        );
        
        /*
         * Populate the cache. We will hold hard references to all of the values
         * in the map inside of this block but only to some of the values in the
         * map outside of this block.
         */
        final int outerSize = 100;
        final int innerSize = 1000000; // 1M
        final Map<Long, String> outerMap = new HashMap<Long, String>();
        {
            final Map<Long, String> innerMap = new HashMap<Long, String>();

            /*
             * Note: Fill the inner map first. This way the references for the
             * inner map will all be batched through the lock.,
             */
            // The inner map
            for (long i = outerSize; i < (outerSize + innerSize); i++) {
                final Long key;
                final String val;
                innerMap.put(key = Long.valueOf(i), val = "" + i);
                fixture.put(key, val);
            }

            assertEquals(0, outerMap.size());
            assertEquals(innerSize, innerMap.size());
            assertEquals(innerSize, fixture.size());
            
            // The outer map.
            for (long i = 0; i < outerSize; i++) {
                final Long key;
                final String val;
                outerMap.put(key = Long.valueOf(-i), val = "outerMap" + i);
                fixture.put(key, val);
            }

            assertEquals(innerSize, innerMap.size());
            assertEquals(outerSize, outerMap.size());
            assertEquals(outerSize + innerSize, fixture.size());
            
            if(log.isInfoEnabled()){
            	log.info("Outer: " + outerMap.size() + " , Inner: " + innerMap.size());
            }

            System.gc();

            if(log.isInfoEnabled()){
            	log.info("Outer: " + outerMap.size() + " , Inner: " + innerMap.size());
            }

            assertEquals(outerSize + innerSize, fixture.size());

//            innerMap.clear(); // clear all entries to facilite GC?
            
        }

        /*
         * Now that the inner map from the block above is out of scope we might
         * be able to force a GC which will clear references which were only
         * retained by that map. To help move things along, we allocate a bunch
         * of stuff and put it into yet another hard reference map.
         */
        {

            final Random r = new Random();

            final Map<Long, String> junkMap = new HashMap<Long, String>();

            while (junkMap.size() < Bytes.megabyte32 * 1) {

                for (int i = 0; i < 100000; i++) {

                    final long n = r.nextLong();

                    junkMap.put(new Long(n), "" + n);

                }

//                System.gc();

                final int fixtureSize = fixture.size();

                if (log.isInfoEnabled())
                    log.info("GC: fixtureSize=" + fixtureSize);

                if (fixture.size() == outerSize) {

                    break;

                }

            }

        }
        
        if(log.isInfoEnabled()) {
            
            /*
             * Dump the contents of the weak value cache.
             */

            final Iterator<Map.Entry<Long, WeakReference<String>>> itr = fixture
                    .entryIterator();

            while (itr.hasNext()) {

                final Map.Entry<Long, WeakReference<String>> e = itr.next();

                log.info(e.getKey() + "\t" + e.getValue().get());

            }

        }

        /*
         * Note: We have to access the outer map after the code which seeks to
         * force a GC in order to have the outer map remain strongly referenced.
         * Without this, the reference winds up being cleared even though we are
         * in the same lexical scope in which the [outerMap] was declared.
         */
        assertEquals(outerSize, outerMap.size());

        /*
         * We should be able to get rid of everything in the inner map.
         */
        assertEquals(outerSize, fixture.size());

    }

}
