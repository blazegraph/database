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
 * Created on Mar 22, 2009
 */

package com.bigdata.counters.store;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase2;

import org.apache.commons.io.FileSystemUtils;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.DefaultInstrumentFactory;
import com.bigdata.counters.ICounter;
import com.bigdata.counters.Instrument;
import com.bigdata.rawstore.Bytes;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestCounterSetBTree extends TestCase2 {

    /**
     * 
     */
    public TestCounterSetBTree() {
    }

    /**
     * @param arg0
     */
    public TestCounterSetBTree(String arg0) {
        super(arg0);
    }

    /**
     * FIXME work through unit tests for writing counters and for querying for
     * the use cases covered by the httpd interface.
     */
    public void test1() {
        
        final CounterSetBTree fixture = CounterSetBTree.createTransient();

        final long fromTime = System.currentTimeMillis();
        
        // root (name := "").
        final CounterSet root = new CounterSet();

        // make a child.
        final CounterSet bigdata = root.makePath("www.bigdata.com");

        // make a child of a child using a relative path
        final ICounter memory = bigdata.addCounter("memory",
                new Instrument<Long>() {
                    public void sample() {
                        setValue(Runtime.getRuntime().freeMemory());
                    }
                });
        
        // make a child of a child using an absolute path.
        final ICounter disk = root.addCounter("/www.bigdata.com/disk",
                new Instrument<Long>() {
                    public void sample() {
                        try {
                        setValue(FileSystemUtils.freeSpaceKb(".")
                                * Bytes.kilobyte);
                        } catch(IOException ex) {
                            log.error(ex,ex);
                        }
                    }
                });

        if (log.isInfoEnabled())
            log.info(root.asXML(null/* filter */));
        
        fixture.insert(root.getCounters(null/* filter */));

        final long toTime = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(1);
        
        final CounterSet tmp = fixture.rangeIterator(fromTime, toTime,
                TimeUnit.MILLISECONDS, null/* filter */,
                TimeUnit.SECONDS/* granularity */,
                DefaultInstrumentFactory.INSTANCE);

        if (log.isInfoEnabled())
            log.info(tmp.asXML(null/* filter */));

        /*
         * FIXME verify same data.
         * 
         * @todo verify with filter.
         * 
         * @todo verify path is decoded.
         * 
         * @todo examine when lots of counters with lots of values for each (~60
         * to a minute).
         * 
         * @todo generalize the units for the aggregation when reading back from
         * the index.
         */
        
    }

}
