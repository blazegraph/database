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
 * Created on Dec 10, 2008
 */

package com.bigdata.service;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import junit.framework.TestCase2;

import com.bigdata.resources.PostProcessOldJournalTask;

/**
 * Unit tests for the round robin behaviors of the load balancer in isolation
 * (not an integration test).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestLoadBalancerRoundRobin extends TestCase2 {

    /**
     * 
     */
    public TestLoadBalancerRoundRobin() {
    }

    /**
     * @param arg0
     */
    public TestLoadBalancerRoundRobin(String arg0) {

        super(arg0);
        
    }

    /**
     * Test when minCount=maxCount=1 and there are N=2 data services to verify
     * correct round robin assignment.
     * 
     * @throws TimeoutException
     * @throws InterruptedException
     */
    public void test_roundRobin_2DataServices() throws InterruptedException,
            TimeoutException {

        final int minCount = 1;
        final int maxCount = 1;
        final UUID exclude = null;
        
        final UUID service1 = UUID.randomUUID();
        final UUID service2 = UUID.randomUUID();
        
        final Set<UUID> services = new HashSet<UUID>();
        services.add(service1);
        services.add(service2);
        
        final IServiceLoadHelper fixture = new AbstractRoundRobinServiceLoadHelper() {

            protected UUID[] awaitServices(int minCount, long timeout)
                    throws InterruptedException, TimeoutException {
            
                return new UUID[] {
                        service1,
                        service2
                };
                
            }
            
        };

        // copy the set of known services.
        Set<UUID> tmp = new HashSet<UUID>(services);

        // Note: 1st round robin assignment (1 out of 2 possible).
        UUID[] actual = fixture.getUnderUtilizedDataServices(minCount,
                maxCount, exclude);

        // verify
        assertNotNull(actual);
        assertEquals(1,actual.length);
        assertTrue(tmp.remove(actual[0]));
        assertTrue(actual[0].equals(service1)||actual[0].equals(service2));
        final UUID expected1 = actual[0];
        
        // Note: 2nd round robin assignment (2 out of 2 possible).
        actual = fixture.getUnderUtilizedDataServices(minCount,
                maxCount, exclude);

        // verify
        assertNotNull(actual);
        assertEquals(1,actual.length);
        assertTrue(tmp.remove(actual[0]));
        assertTrue(actual[0].equals(service1)||actual[0].equals(service2));
        final UUID expected2 = actual[0];
        
        for (int i = 0; i < 10; i++) {
            
            // Note: should always deliver [expected1].
            actual = fixture.getUnderUtilizedDataServices(minCount,
                    maxCount, exclude);
            
            assertNotNull(actual);
            assertEquals(1, actual.length);
            assertEquals(expected1, actual[0]);
            
            // Note: should always deliver [expected2].
            actual = fixture.getUnderUtilizedDataServices(minCount,
                    maxCount, exclude);
            
            assertNotNull(actual);
            assertEquals(1, actual.length);
            assertEquals(expected2, actual[0]);
            
        }
        
    }
    
    /**
     * Test when minCount=maxCount=1 and there are N GT 2 data services to
     * verify correct round robin assignment.
     * 
     * @throws TimeoutException
     * @throws InterruptedException
     */
    public void test_roundRobin_NDataServices() throws InterruptedException,
            TimeoutException {

        final int minCount = 1;
        final int maxCount = 1;
        final UUID exclude = null;
       
        final int N = 12;
        
        final Set<UUID> services = new HashSet<UUID>();
        for (int i = 0; i < N; i++) {
            services.add(UUID.randomUUID());
        }
        
        final IServiceLoadHelper fixture = new AbstractRoundRobinServiceLoadHelper() {

            protected UUID[] awaitServices(int minCount, long timeout)
                    throws InterruptedException, TimeoutException {
            
                return services.toArray(new UUID[]{});
                
            }
            
        };

        // copy the set of known services.
        Set<UUID> tmp = new HashSet<UUID>(services);

        // the actual order in which the services are assigned.
        final UUID[] expected = new UUID[N];

        // figure out what order the round robin is using.
        for (int i = 0; i < N; i++) {

            // Note: round robin assignment.
            final UUID[] actual = fixture.getUnderUtilizedDataServices(minCount,
                    maxCount, exclude);

            // verify
            assertNotNull(actual);
            assertEquals(1, actual.length);
            assertTrue(tmp.remove(actual[0]));
            assertTrue(services.contains(actual[0]));
            expected[i] = actual[0];

        }

        /* 
         * Verify that it continues to use that order.
         */
        // M trials
        for (int i = 0; i < 10; i++) {

            // verify the complete order for this trial.
            for (int j = 0; j < N; j++) {

                final UUID[] actual = fixture.getUnderUtilizedDataServices(
                        minCount, maxCount, exclude);

                assertNotNull(actual);
                assertEquals(1, actual.length);
                assertEquals(expected[j], actual[0]);

            }

        }
        
    }
    
    /**
     * Test when minCount=maxCount=0 where there is an excluded service. This is
     * how {@link PostProcessOldJournalTask} looks for target services for index
     * partition MOVEs. {@link PostProcessOldJournalTask} will specify the local
     * data service as the excluded service. The proper behavior is to return
     * all services except the excluded service on each request.
     * 
     * @throws TimeoutException 
     * @throws InterruptedException 
     */
    public void test_excluded() throws InterruptedException, TimeoutException {

        final int minCount = 0;
        final int maxCount = 0;

        for (int N = 2; N < 20; N++) {

            final UUID exclude = UUID.randomUUID();

            final Set<UUID> services = new HashSet<UUID>();

            services.add(exclude);

            for (int i = 0; i < N - 1; i++) {

                services.add(UUID.randomUUID());

            }

            assertEquals(N, services.size());

            final IServiceLoadHelper fixture = new AbstractRoundRobinServiceLoadHelper() {

                protected UUID[] awaitServices(int minCount, long timeout)
                        throws InterruptedException, TimeoutException {

                    return services.toArray(new UUID[] {});

                }

            };

            // do a number of trials
            for (int i = 0; i < 10; i++) {

                // Note: round robin assignment.
                final UUID[] actual = fixture.getUnderUtilizedDataServices(
                        minCount, maxCount, exclude);

                // verify
                assertNotNull(actual);
                for (UUID x : actual) {
                    assertNotNull(x);
                    assertTrue(services.contains(x));
                    assertFalse(x.equals(exclude));
                }
                assertEquals(N - 1, actual.length);

            }

        }

    }

}
