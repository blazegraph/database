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
package com.bigdata.journal;

import java.util.UUID;

import junit.framework.TestCase2;

import com.bigdata.util.ClocksNotSynchronizedException;

/**
 * Test suite for {@link ClocksNotSynchronizedException}. The basic pattern of
 * events is as follows:
 * 
 * <pre>
 * leader   : t1          : timestamp before gather() messages are sent to followers.
 * follower :     t2      : timestamp taken when servicing gather() message and sent to leader with response.
 * leader   :         t3  : timestamp taken on leader when barrier breaks.
 * </pre>
 * 
 * Of necessity, these events have a temporal order (t1 BEFORE t2; t2 BEFORE
 * t3). However, there can be skew in the clocks such that the clock on the
 * leader and the clock on the follower(s) are not synchronized. Some clock skew
 * is allowed, but significant clock skew can cause a problem on failover.
 * <p>
 * The problem arises because the clocks are used to assign timestamps for
 * commit points, and we index into the journal using those timestamps for
 * historical reads (reading on the database as of some wall clock time).
 * <p>
 * {@link AbstractJournal#commitNow(long)} does ensure that time moves forward
 * relative to the timestamp associated with the last commit point on the
 * journal. However, if the skew is large, then this could require waiting for
 * minutes, hours, or days before a new commit time could be assigned.
 * <p>
 * In order to avoid such long latency during failover, an error is reported
 * proactively if a large clock skew is detected during the release time
 * consensus protocol.
 * <p>
 * This test suite verifies the logic for detecting clock skew.
 * 
 * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/686" >
 *      Consensus protocol does not detect clock skew correctly </a>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestClockSkewDetection extends TestCase2 {

    public TestClockSkewDetection() {
    }

    public TestClockSkewDetection(String name) {
        super(name);
    }

    private UUID serviceId1, serviceId2;
    private final static long maxSkew = 50; // ms.

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        serviceId1 = UUID.randomUUID();
        serviceId2 = UUID.randomUUID();
    }
    
    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        serviceId1 = serviceId2 = null;
    }

    /**
     * Helper calls through to assertBefore().
     * @param t1
     * @param t2
     */
    private void assertBefore(final long t1, final long t2) {

        ClocksNotSynchronizedException.assertBefore(serviceId1, serviceId2, t1,
                t2, maxSkew);

    }
    
    /**
     * Helper fails if assertBefore() succeeds.
     * @param t1
     * @param t2
     */
    private void assertNotBefore(final long t1, final long t2) {
        
        try {
            
            assertBefore(t1, t2);
            
            fail("Not expecting t1(" + t1 + ") to be 'before' t2(" + t2 + ")");

        } catch(ClocksNotSynchronizedException ex) {
        
            if(log.isInfoEnabled())
                log.info("Ignoring expected exception: "+ex);
            
        }
        
    }

    /*
     * Tests where [t1 LT t2].
     */

    /**
     * Tests where the delta is LT {@value #maxSkew} and <code>t1 LT t2</code>
     */
    public void test01() {

        final long delta = 10;

        assertTrue(delta < maxSkew);

        assertBefore(200 - delta, 200);

        assertBefore(300 - delta, 300);

    }
    
    /**
     * Tests where the delta is EQ {@value #maxSkew} and <code>t1 LT t2</code>
     */
    public void test02() {
        
        final long delta = maxSkew;
        
        assertBefore(200 - delta, 200);

        assertBefore(300 - delta, 300);

    }
    
    /**
     * Tests where the delta is GT {@value #maxSkew} and <code>t1 LT t2</code>
     */
    public void test03() {

        final long delta = 60;

        assertTrue(delta > maxSkew);

        assertBefore(100 - delta, 200);

        assertBefore(200 - delta, 300);

    }

    /*
     * Tests where [t1 GTE t2].
     */
    
    /**
     * Tests where the delta is LT {@value #maxSkew} and <code>t1 GTE t2</code>.
     * <p>
     * Note: This is a test for a "fuzzy" sense of "before". We explicitly allow
     * for some clock skew since it will not cause a significantly latency on
     * failover and minor clock skew (on the order of the latency of an RMI) is
     * common, even with synchronized clocks.
     */
    public void test11() {
        
        final long delta = 10;

        assertTrue(delta < maxSkew);

        assertBefore(200 + delta, 200);

        assertBefore(300 + delta, 300);
        
    }

    /**
     * Tests where the delta is EQ {@value #maxSkew} and <code>t1 GTE t2</code>
     */
    public void test12() {
        
        final long delta = maxSkew;
        
        assertBefore(200 + delta, 200);

        assertBefore(300 + delta, 300);

    }
    
    /**
     * Tests where the delta is GT {@value #maxSkew} and <code>t1 GTE t2</code>
     */
    public void test13() {

        final long delta = 60;

        assertTrue(delta > maxSkew);

        assertNotBefore(200 + delta, 200);

        assertNotBefore(300 + delta, 300);

    }

}
