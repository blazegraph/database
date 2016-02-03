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
package com.bigdata.rdf.sail.webapp.lbs;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase2;

import com.bigdata.BigdataStatics;
import com.bigdata.util.httpd.Config;

/**
 * Test suite for the basic stochastic load balancing mechanism for LBS policies
 * based on actual host workloads regardless of how those workload metrics are
 * obtained.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 *         TODO Test load balancing for no hosts, one host (with 1 service, 2
 *         services, 3 services) and 3 hosts (each with one service). Also test
 *         cases where some hosts have no active services, e.g., 3 hosts but
 *         only one is active or 3 hosts but only 2 are active.
 */
public class TestAbstractHostLBSPolicy extends TestCase2 {

    private AtomicInteger nextPort;

    @Override
    protected void setUp() throws Exception {
        nextPort = new AtomicInteger(Config.BLAZEGRAPH_HTTP_PORT);
      
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        nextPort = null;
    }

    public TestAbstractHostLBSPolicy() {
    }

    public TestAbstractHostLBSPolicy(String name) {
        super(name);
    }

    /**
     * This test covers the case where there is only one host and it is running
     * one or more services.
     */
    public void test_lbs_host_policy() {

        // The hostnames.
        final String H1 = "H1";
        
        // The load scores for those hosts.
        final HostScore hostScore1 = new HostScore("H1", 1d);

        // should sum to 1.0
        assertEquals(1d, hostScore1.getAvailability());

        final HostScore[] hostScores = new HostScore[] { //
        hostScore1 //
        };

        {
            final HostScore actualHost = AbstractHostLBSPolicy.getHost(.0,
                    hostScores);

            // Only one host.
            assertSameRef(hostScore1, actualHost);
        }

        {
            final HostScore actualHost = AbstractHostLBSPolicy.getHost(.1,
                    hostScores);

            // Only one host.
            assertSameRef(hostScore1, actualHost);
        }

        {
            final HostScore actualHost = AbstractHostLBSPolicy.getHost(.5,
                    hostScores);

            // Only one host.
            assertSameRef(hostScore1, actualHost);
        }

        {
            final HostScore actualHost = AbstractHostLBSPolicy.getHost(.9,
                    hostScores);

            // Only one host.
            assertSameRef(hostScore1, actualHost);
        }

        {
            try {
                AbstractHostLBSPolicy.getHost(1d, hostScores);
            } catch (IllegalArgumentException ex) {
                // ignore.
                if (log.isInfoEnabled())
                    log.info(ex);
            }
        }

        {
            try {
                AbstractHostLBSPolicy.getHost(-.00001d, hostScores);
            } catch (IllegalArgumentException ex) {
                // ignore.
                if (log.isInfoEnabled())
                    log.info(ex);
            }
        }

        // The services.
        final UUID A = UUID.randomUUID();
        final UUID B = UUID.randomUUID();
        final UUID C = UUID.randomUUID();
        
        final ServiceScore serviceA = new ServiceScore(A, H1, toRequestURI(H1));
        final ServiceScore serviceB = new ServiceScore(B, H1, toRequestURI(H1));
        final ServiceScore serviceC = new ServiceScore(C, H1, toRequestURI(H1));

        /*
         * Now check the method to identify a specific joined service known to
         * be running on that host.
         */
        {
            {

                // Run with a known seed.
                final Random rand = new Random(1L);

                final ServiceScore[] serviceScores = new ServiceScore[] { //
                serviceA //
                };

                final ServiceScore actualService = AbstractHostLBSPolicy
                        .getService(rand, hostScore1, serviceScores);

                assertTrue(actualService == serviceA);

            }

            {

                // Run with a known seed.
                final Random rand = new Random(1L);

                final ServiceScore[] serviceScores = new ServiceScore[] { //
                serviceA, serviceB, serviceC
                };

                final ServiceScore actualService = AbstractHostLBSPolicy
                        .getService(rand, hostScore1, serviceScores);

                assertTrue(actualService == serviceA);

            }

        }

    }
    
    /**
     * This test covers the case with 3 hosts, each running one service.
     */
    public void test_HA3() {
//    
//        /*
//         * The load scores for those hosts.
//         * 
//         * Note: These need to be ordered by increasing load. That is how they
//         * are sorted by the LBS code. The running totals in terms of the
//         * normalized load.
//         */
//        final double totalRawScore = 1.5d;
//        final HostScore hostScore0 = new HostScore("H0", .2d); // score=.533
//        final HostScore hostScore1 = new HostScore("H1", .5d); // score=.333, total=.866
//        final HostScore hostScore2 = new HostScore("H2", .8d); // score=.133, total=1.00
//
//        /*
//         * Show scores. Decision is made on the *normalized* score. Since we
//         * want to assign work based on inverse load, "normalized" is defined as
//         * (1-load)/total
//         */
//        log.warn("H1=" + hostScore0);
//        log.warn("H2=" + hostScore1);
//        log.warn("H3=" + hostScore2);
//
//        // verify total of the raw scores.
//        assertEquals(totalRawScore, hostScore0.getRawScore() + hostScore1.getRawScore()
//                + hostScore2.getRawScore());
//        
//        // verify individual normalized scores.
//        assertEquals(hostScore0.getScore(), (1d-hostScore0.getRawScore()) / totalRawScore);
//        assertEquals(hostScore1.getScore(), (1d-hostScore1.getRawScore()) / totalRawScore);
//        assertEquals(hostScore2.getScore(), (1d-hostScore2.getRawScore()) / totalRawScore);
//
//        // verify total of the normalized scores.
//        assertEquals(1d, hostScore0.getScore() + hostScore1.getScore() + hostScore2.getScore());
//
//        // Arrange host scores in some perturbed order.
//        final HostScore[] hostScores = new HostScore[] { hostScore2,
//                hostScore0, hostScore1 };
//        
//        // Sort by increasing normalized score (decreasing free capacity).
//        Arrays.sort(hostScores);
//
//        log.warn("hostScores[0]=" + hostScores[0]);
//        log.warn("hostScores[1]=" + hostScores[1]);
//        log.warn("hostScores[2]=" + hostScores[2]);
//
//        // verify imposed ordering on (1-load).
//        assertSameRef(hostScores[2], hostScore0); // most  load (1-load = .533)
//        assertSameRef(hostScores[1], hostScore1); // middle load (1-load = .333)
//        assertSameRef(hostScores[0], hostScore2); // least   load (1-load = .133)
//        
//        // Verify ordering (increasing normalized score).
//        assertTrue(hostScores[0].getScore() < hostScores[1].getScore());
//        assertTrue(hostScores[1].getScore() < hostScores[2].getScore());
//
//        final double threshold0 = 0d;
//        final double threshold1 = hostScores[0].getScore();
//        final double threshold2 = threshold1 + hostScores[1].getScore();
//        log.warn("threshold0=" + threshold0);
//        log.warn("threshold1=" + threshold1);
//        log.warn("threshold2=" + threshold2);
//        
//        {
//            final HostScore actualHost = AbstractHostLBSPolicy.getHost(
//                    threshold0, hostScores);
//
//            assertSameRef(hostScores[0], actualHost);
//        }
//
//        {
//            final HostScore actualHost = AbstractHostLBSPolicy.getHost(
//                    (threshold1 - .001), hostScores);
//
//            assertSameRef(hostScores[0], actualHost);
//        }
//
//        {
//            final HostScore actualHost = AbstractHostLBSPolicy.getHost(
//                    threshold1, hostScores);
//
//            assertSameRef(hostScores[1], actualHost);
//        }
//
//        {
//            final HostScore actualHost = AbstractHostLBSPolicy.getHost(
//                    (threshold2 - .001), hostScores);
//
//            assertSameRef(hostScores[1], actualHost);
//        }
//
//        {
//            final HostScore actualHost = AbstractHostLBSPolicy.getHost(
//                    threshold2, hostScores);
//
//            assertSameRef(hostScores[2], actualHost);
//        }
//
//        {
//            final HostScore actualHost = AbstractHostLBSPolicy.getHost(
//                    1d - .0001, hostScores);
//
//            assertSameRef(hostScores[2], actualHost);
//        }
//
//        {
//            try {
//                AbstractHostLBSPolicy.getHost(1d, hostScores);
//            } catch (IllegalArgumentException ex) {
//                // ignore.
//                if (log.isInfoEnabled())
//                    log.info(ex);
//            }
//        }
//
//        {
//            try {
//                AbstractHostLBSPolicy.getHost(-.00001d, hostScores);
//            } catch (IllegalArgumentException ex) {
//                // ignore.
//                if (log.isInfoEnabled())
//                    log.info(ex);
//            }
//        }
//
//        // The services.
//        final UUID A = UUID.randomUUID();
//        final UUID B = UUID.randomUUID();
//        final UUID C = UUID.randomUUID();
//        
//        final ServiceScore serviceA = new ServiceScore(A, hostScore0.getHostname(), toRequestURI(hostScore0.getHostname()));
//        final ServiceScore serviceB = new ServiceScore(B, hostScore1.getHostname(), toRequestURI(hostScore1.getHostname()));
//        final ServiceScore serviceC = new ServiceScore(C, hostScore2.getHostname(), toRequestURI(hostScore2.getHostname()));
//
//        /*
//         * Now check the method to identify a specific joined service known to
//         * be running on that host.
//         */
//        {
//            {
//
//                // Run with a known seed.
//                final Random rand = new Random(1L);
//
//                final ServiceScore[] serviceScores = new ServiceScore[] { //
//                serviceA //
//                };
//
//                final ServiceScore actualService = AbstractHostLBSPolicy
//                        .getService(rand, hostScore0, serviceScores);
//
//                assertTrue(actualService == serviceA);
//
//            }
//
//            {
//
//                // Run with a known seed.
//                final Random rand = new Random(1L);
//
//                final ServiceScore[] serviceScores = new ServiceScore[] { //
//                serviceA, serviceB, serviceC
//                };
//
//                final ServiceScore actualService = AbstractHostLBSPolicy
//                        .getService(rand, hostScore0, serviceScores);
//
//                assertTrue(actualService == serviceA);
//
//            }
//
//        }

    }

    /**
     * Verify that two references are the same reference (<code>==</code>).
     * 
     * @param expected
     *            The expected reference.
     * @param actual
     *            The actual reference.
     */
    private <T> void assertSameRef(final T expected, final T actual) {
       
        if (expected != actual) {

            fail("Different reference: expected=" + expected + ", actual=" + actual);
            
        }
        
    }

    /**
     * Hacks together a Request-URI for a service.
     * 
     * @param hostname
     * @return
     */
    private String toRequestURI(final String hostname) {

        return "http://" + hostname + ":" + nextPort.getAndIncrement()
                + BigdataStatics.getContextPath();

    }

}
