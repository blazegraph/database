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
import java.io.InputStream;
import java.io.StringWriter;
import java.util.concurrent.TimeUnit;

import javax.xml.parsers.ParserConfigurationException;

import junit.framework.TestCase2;

import org.apache.commons.io.FileSystemUtils;
import org.xml.sax.SAXException;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.DefaultInstrumentFactory;
import com.bigdata.counters.History;
import com.bigdata.counters.HistoryInstrument;
import com.bigdata.counters.ICounter;
import com.bigdata.counters.IHistoryEntry;
import com.bigdata.counters.Instrument;
import com.bigdata.counters.PeriodEnum;
import com.bigdata.counters.History.SampleIterator;
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
        
        fixture.writeHistory(root.getCounters(null/* filter */));

        /*
         * Note: The toTime needs to be ONE (1) minute beyond the time of
         * interest since the minutes come first in the key.
         */
        final long toTime = fromTime + TimeUnit.MINUTES.toMillis(1);

        final CounterSet tmp = fixture.rangeIterator(fromTime, toTime,
                TimeUnit.MINUTES, null/* filter */, 0/* depth */);

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

    
    /**
     * Unit test reads some known data from a local test resource.
     * 
     * @throws SAXException
     * @throws ParserConfigurationException
     * @throws IOException
     */
    public void test_XML() throws IOException, ParserConfigurationException,
            SAXException {

        final CounterSetBTree fixture = CounterSetBTree.createTransient();

        final CounterSet expected = new CounterSet();

        final InputStream is = getClass().getResourceAsStream(
                "counters-test0.xml");

        assertNotNull("Could not locate resource", is);

        try {
            /*
             * Note: This will throw a runtime exception if a source file
             * contains more than 60 minutes worth of history data.
             */
            expected
                    .readXML(is, new DefaultInstrumentFactory(60/* nslots */,
                            PeriodEnum.Minutes, false/* overwrite */), null/* filter */);

        } finally {

            is.close();

        }

        if (log.isInfoEnabled()) {
            final StringWriter w = new StringWriter();
            expected.asXML(w, null/* filter */);
            log.info("expected:\n" + w);
        }

        if (log.isInfoEnabled())
            log.info("Writing counters on store.");

        fixture.writeHistory(expected.getCounters(null/* filter */));

        final CounterSet actual = fixture
                .rangeIterator(0L/* fromTime */, 0L/* toTime */,
                        TimeUnit.MINUTES, null/* filter */, 0/* depth */);

        if (log.isInfoEnabled()) {
            final StringWriter w = new StringWriter();
            actual.asXML(w, null/* filter */);
            log.info("actual:\n" + w);
        }

        final String path = "/blade10.dpp2.org/CPU/% Processor Time";
        
        assertNotNull(path, expected.getPath(path));
        assertNotNull(path, actual.getPath(path));
        assertTrue(expected.getPath(path) instanceof ICounter);
        assertTrue(actual.getPath(path) instanceof ICounter);
        assertTrue(((ICounter) expected.getPath(path)).getInstrument() instanceof HistoryInstrument);
        assertTrue(((ICounter) actual.getPath(path)).getInstrument() instanceof HistoryInstrument);
        
        final History<Double> expectedHistory = ((HistoryInstrument<Double>) ((ICounter) expected
                .getPath(path)).getInstrument()).getHistory();

        final History<Double> actualHistory = ((HistoryInstrument<Double>) ((ICounter) actual
                .getPath(path)).getInstrument()).getHistory();

        assertSameHistory(expectedHistory, actualHistory);

    }

    protected static void assertSameHistory(final History expected,
            final History actual) {

        assertEquals("period", expected.getPeriod(), actual.getPeriod());

        /*
         * Note: Can't compare capacity since they are allocated by different
         * sections of the code for different purposes.
         */
//        assertEquals("capacity", expected.capacity(), actual.capacity());

        assertEquals("size", expected.size(), actual.size());
        
        assertEquals("valueType", expected.getValueType(), actual
                .getValueType());
        
        final SampleIterator esitr = expected.iterator();

        final SampleIterator asitr = actual.iterator();
        
        assertEquals("firstSampleTime", esitr.getFirstSampleTime(), asitr
                .getFirstSampleTime());

        assertEquals("lastSampleTime", esitr.getLastSampleTime(), asitr
                .getLastSampleTime());

        while (esitr.hasNext()) {

            final IHistoryEntry eEntry = esitr.next();

            assert (asitr.hasNext());

            final IHistoryEntry aEntry = asitr.next();

            /*
             * Note: Can't compare total of the #of samples in a slot or the
             * count of the #of samples in a slot since those data are not
             * serialized in the XML representation.
             */
            
//            assertEquals("total", eEntry.getTotal(), aEntry.getTotal());
//
//            assertEquals("count", eEntry.getCount(), aEntry.getCount());

            assertEquals("average", eEntry.getValue(), aEntry.getValue());

        }
        
        assertFalse("Actual visits too many entries.", asitr.hasNext());
        
    }
    
}
